import streamlit as st
import pandas as pd
import requests
import math
from datetime import datetime, date
import io
from sqlalchemy import create_engine, text

# -----------------------------
# НАСТРОЙКИ (Secrets в Streamlit Cloud)
# -----------------------------
TOKEN = st.secrets["MS_TOKEN"]
ORG_ID = st.secrets.get("MS_ORG_ID", "")  # сейчас не используется, оставили на будущее
STORE_ID = st.secrets["MS_STORE_ID"]
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# -----------------------------
# БАЗА ДАННЫХ
# -----------------------------
DB_URL = st.secrets.get("DB_URL", "sqlite:///warehouse.db")
engine = create_engine(DB_URL)

# -----------------------------
# ЕЖЕДНЕВНОЕ ЛОГИРОВАНИЕ ХРАНЕНИЯ (23:00+)
# Таблица в Neon по инструкции:
# (log_date, boxes_ip, pallets_ip, cost_ip, boxes_ooo, pallets_ooo, cost_ooo, total_cost)
# -----------------------------
def check_and_log_daily():
    now = datetime.now()
    if now.hour >= 23:
        today_str = now.strftime("%Y-%m-%d")
        with engine.connect() as conn:
            # Проверяем, была ли запись за сегодня
            res = conn.execute(
                text("SELECT 1 FROM daily_storage_logs WHERE log_date = :d"),
                {"d": today_str},
            ).fetchone()

            if not res:
                # Если нет — считаем и записываем
                df = pd.read_sql(text("SELECT * FROM stock"), engine)
                df["type"] = df["type"].replace({"000": "ООО"})

                b_ip = int((df["type"] == "ИП").sum()) if not df.empty else 0
                b_ooo = int((df["type"] == "ООО").sum()) if not df.empty else 0

                # 16 коробов = 1 паллет
                p_ip = int(math.ceil(b_ip / 16)) if b_ip else 0
                p_ooo = int(math.ceil(b_ooo / 16)) if b_ooo else 0

                cost_ip = p_ip * 50
                cost_ooo = p_ooo * 50
                total_cost = cost_ip + cost_ooo

                conn.execute(
                    text(
                        """
                        INSERT INTO daily_storage_logs
                        (log_date, boxes_ip, pallets_ip, cost_ip, boxes_ooo, pallets_ooo, cost_ooo, total_cost)
                        VALUES (:d, :bi, :pi, :ci, :bo, :po, :co, :tc)
                        """
                    ),
                    {
                        "d": today_str,
                        "bi": b_ip,
                        "pi": p_ip,
                        "ci": cost_ip,
                        "bo": b_ooo,
                        "po": p_ooo,
                        "co": cost_ooo,
                        "tc": total_cost,
                    },
                )
                conn.commit()


def init_db():
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS stock (
                    uuid TEXT PRIMARY KEY,
                    name TEXT,
                    article TEXT,
                    barcode TEXT,
                    quantity REAL,
                    box_num TEXT,
                    type TEXT
                )
                """
            )
        )

        # archive расширили: ship_date, fio, ship_store
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS archive (
                    uuid TEXT PRIMARY KEY,
                    name TEXT,
                    article TEXT,
                    barcode TEXT,
                    quantity REAL,
                    box_num TEXT,
                    type TEXT,
                    ship_date TEXT,
                    fio TEXT,
                    ship_store TEXT
                )
                """
            )
        )

        # если archive уже был — добавим столбцы (для Postgres/Neon)
        try:
            conn.execute(text("ALTER TABLE archive ADD COLUMN IF NOT EXISTS ship_date TEXT"))
            conn.execute(text("ALTER TABLE archive ADD COLUMN IF NOT EXISTS fio TEXT"))
            conn.execute(text("ALTER TABLE archive ADD COLUMN IF NOT EXISTS ship_store TEXT"))
        except Exception:
            pass

        conn.commit()


# Инициализация БД
init_db()

# Ежедневное логирование (в конце дня)
try:
    check_and_log_daily()
except Exception:
    # если таблица daily_storage_logs ещё не создана — не валим приложение
    pass

# -----------------------------
# UI
# -----------------------------
st.set_page_config(layout="wide", page_title="Складской Терминал")

if "reset_counter" not in st.session_state:
    st.session_state.reset_counter = 0

def reset_selection():
    st.session_state.reset_counter += 1

def load_api_data():
    url = (
        "https://api.moysklad.ru/api/remap/1.2/report/stock/all"
        f"?limit=1000&filter=store=https://api.moysklad.ru/api/remap/1.2/entity/store/{STORE_ID}"
    )
    try:
        res = requests.get(url, headers=HEADERS, timeout=30)
        return res.json().get("rows", []) if res.status_code == 200 else []
    except Exception:
        return []

ms_rows = load_api_data()
api_status = "🟢 Связь с МойСклад: Установлена" if ms_rows else "🔴 Связь с МойСклад: Ошибка"

st.title("📦 Единая база склада (ИП / ООО)")
st.caption(api_status)

# -----------------------------
# Сайдбар: Приемка + Инвентаризация
# -----------------------------
with st.sidebar:
    st.header("📥 Приемка")
    uploaded_file = st.file_uploader("Загрузи Excel (Баркод, Кол-во, Короб)", type=["xlsx"])
    target_type = st.radio("Тип поставки:", ["ИП", "ООО"])

    if uploaded_file and st.button("➕ Добавить на баланс"):
        try:
            new_data = pd.read_excel(uploaded_file)
            new_data.columns = ["Баркод", "Кол-во", "Номер короба"]

            mapping = {
                str(r.get("code")): (r.get("article", "-"), r.get("name", "Неизвестно"))
                for r in ms_rows
            }

            with engine.connect() as conn:
                for i, row in new_data.iterrows():
                    art, name = mapping.get(str(row["Баркод"]), ("-", "Новый товар"))
                    uid = f"ID_{datetime.now().timestamp()}_{row['Баркод']}_{i}"
                    conn.execute(
                        text(
                            """
                            INSERT INTO stock (uuid, name, article, barcode, quantity, box_num, type)
                            VALUES (:u, :n, :a, :b, :q, :bn, :t)
                            """
                        ),
                        {
                            "u": str(uid),
                            "n": str(name),
                            "a": str(art),
                            "b": str(row["Баркод"]),
                            "q": float(row["Кол-во"]),
                            "bn": str(row["Номер короба"]),
                            "t": str(target_type),
                        },
                    )
                conn.commit()

            reset_selection()
            st.success("Данные сохранены!")
            st.rerun()
        except Exception as e:
            st.error(f"Ошибка: {e}")

    st.divider()
    st.header("📦 Отчёты")

    # Инвентаризация: выгружаем все остатки ИП и ООО без удаления
    try:
        df_all_inv = pd.read_sql(text("SELECT * FROM stock"), engine)
        df_all_inv["type"] = df_all_inv["type"].replace({"000": "ООО"})
    except Exception:
        df_all_inv = pd.DataFrame()

    inv_out = io.BytesIO()
    with pd.ExcelWriter(inv_out, engine="xlsxwriter") as writer:
        if not df_all_inv.empty:
            inv_ip = df_all_inv[df_all_inv["type"] == "ИП"][["barcode", "quantity", "box_num", "article", "name", "type"]].copy()
            inv_ooo = df_all_inv[df_all_inv["type"] == "ООО"][["barcode", "quantity", "box_num", "article", "name", "type"]].copy()
        else:
            inv_ip = pd.DataFrame(columns=["barcode", "quantity", "box_num", "article", "name", "type"])
            inv_ooo = pd.DataFrame(columns=["barcode", "quantity", "box_num", "article", "name", "type"])

        for d in (inv_ip, inv_ooo):
            d.columns = ["Баркод", "Количество", "Номер короба", "Артикул", "Наименование", "Юр лицо"]

        inv_ip.to_excel(writer, index=False, sheet_name="ИП")
        inv_ooo.to_excel(writer, index=False, sheet_name="ООО")

    st.download_button(
        "📤 Инвентаризация (скачать остатки)",
        data=inv_out.getvalue(),
        file_name=f"inventory_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# -----------------------------
# Поиск + вкладки
# -----------------------------
search = st.text_input("🔍 Быстрый поиск (Баркод / Артикул / Короб / Наименование)")

t1, t2, t3, t4, t5 = st.tabs(["🏠 ИП", "🏢 ООО", "📜 Архив", "💰 Хранение", "📊 Итого"])

def make_view_stock(df: pd.DataFrame) -> pd.DataFrame:
    """Витрина для отображения: Баркод, Количество, Номер короба, Артикул, Наименование, Юр лицо"""
    if df.empty:
        return df
    df = df.copy()
    df["type"] = df["type"].replace({"000": "ООО"})
    view = df[["barcode", "quantity", "box_num", "article", "name", "type"]].copy()
    view.columns = ["Баркод", "Количество", "Номер короба", "Артикул", "Наименование", "Юр лицо"]
    return view

def apply_search(df: pd.DataFrame, query: str) -> pd.DataFrame:
    if df.empty or not query:
        return df
    q = query.strip()
    mask = df.astype(str).apply(lambda col: col.str.contains(q, case=False, na=False)).any(axis=1)
    return df[mask]

def shipment_ui(selected_rows: pd.DataFrame, storage_type: str, key: str):
    """Окно ввода параметров отгрузки + скачивание + перенос в архив + удаление из stock."""
    def build_excel(fio: str, ship_store: str, ship_date: date) -> bytes:
        exp_df = selected_rows[["barcode", "quantity", "box_num", "article", "name"]].copy()
        exp_df.columns = ["Баркод", "Количество", "Номер короба", "Артикул", "Наименование"]
        exp_df["Юр лицо"] = storage_type
        exp_df["ФИО"] = fio
        exp_df["Склад отгрузки"] = ship_store
        exp_df["Дата отгрузки"] = ship_date.strftime("%d.%m.%Y")

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            exp_df.to_excel(writer, index=False, sheet_name="Отгрузка")
            ws = writer.sheets["Отгрузка"]
            ws.freeze_panes(1, 0)
            ws.set_column(0, 0, 18)
            ws.set_column(1, 1, 12)
            ws.set_column(2, 2, 16)
            ws.set_column(3, 4, 28)
            ws.set_column(5, 7, 20)
        return output.getvalue()

    def confirm_and_ship(fio: str, ship_store: str, ship_date: date, idx_list: list[int], df_source: pd.DataFrame):
        with engine.connect() as conn:
            for i in idx_list:
                r = df_source.iloc[i]
                conn.execute(
    text("""
        INSERT INTO archive (uuid, name, article, barcode, quantity, box_num, type, ship_date, fio, ship_store)
        VALUES (:u, :n, :a, :b, :q, :bn, :t, :sd, :fio, :ss)
        ON CONFLICT (uuid) DO UPDATE SET
            name = EXCLUDED.name,
            article = EXCLUDED.article,
            barcode = EXCLUDED.barcode,
            quantity = EXCLUDED.quantity,
            box_num = EXCLUDED.box_num,
            type = EXCLUDED.type,
            ship_date = EXCLUDED.ship_date,
            fio = EXCLUDED.fio,
            ship_store = EXCLUDED.ship_store
    """),
    params_dict
)
                conn.execute(text("DELETE FROM stock WHERE uuid=:u"), {"u": r["uuid"]})
            conn.commit()

    # Поддержка st.dialog, а если её нет — показываем форму прямо на странице
    if hasattr(st, "dialog"):
        @st.dialog("Параметры отгрузки")
        def _dialog():
            fio = st.text_input("ФИО")
            ship_store = st.text_input("Склад отгрузки")
            ship_date = st.date_input("Дата отгрузки", value=datetime.now().date())

            if not fio.strip() or not ship_store.strip():
                st.info("Заполни ФИО и склад отгрузки — без этого отгрузку подтвердить нельзя.")

            excel_bytes = build_excel(fio, ship_store, ship_date)

            if st.download_button(
                f"⬇️ Скачать и подтвердить отгрузку",
                data=excel_bytes,
                file_name=f"shipment_{storage_type}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_ship_{key}_{st.session_state.reset_counter}",
                disabled=not (fio.strip() and ship_store.strip()),
            ):
                # Важно: перенос в архив + удаление из stock делаем в месте вызова (там есть idx/df)
                st.session_state[f"_ship_confirm_{key}"] = {
                    "fio": fio,
                    "ship_store": ship_store,
                    "ship_date": ship_date.strftime("%Y-%m-%d"),
                }
                st.rerun()

        _dialog()
    else:
        with st.expander("Параметры отгрузки", expanded=True):
            fio = st.text_input("ФИО", key=f"fio_{key}")
            ship_store = st.text_input("Склад отгрузки", key=f"store_{key}")
            ship_date = st.date_input("Дата отгрузки", value=datetime.now().date(), key=f"date_{key}")
            excel_bytes = build_excel(fio, ship_store, ship_date)
            st.download_button(
                "⬇️ Скачать файл отгрузки",
                data=excel_bytes,
                file_name=f"shipment_{storage_type}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                disabled=not (fio.strip() and ship_store.strip()),
                key=f"dl_ship_fallback_{key}_{st.session_state.reset_counter}",
            )

def render_table(storage_type: str, key: str):
    df = pd.read_sql(text("SELECT * FROM stock WHERE type=:t"), engine, params={"t": storage_type})
    df["type"] = df["type"].replace({"000": "ООО"})

    df = apply_search(df, search)

    view = make_view_stock(df)

    if not df.empty:
        table_key = f"table_{key}_{st.session_state.reset_counter}"
        sel = st.dataframe(
            view,
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="multi-row",
            key=table_key,
        )
        idx = sel.get("selection", {}).get("rows", [])

        if idx:
            c1, c2 = st.columns(2)

            selected_rows = df.iloc[idx].copy()

            # ОТГРУЗКА: открываем окно ввода параметров, затем скачивание + перенос в архив
            if c1.button(f"🚀 Отгрузка ({len(idx)})", key=f"open_ship_{key}"):
                st.session_state[f"_open_ship_{key}"] = True

            if st.session_state.get(f"_open_ship_{key}", False):
                shipment_ui(selected_rows, storage_type, key)

                # Если диалог подтвердили (см. shipment_ui), делаем перенос и чистим состояние
                payload = st.session_state.get(f"_ship_confirm_{key}")
                if payload:
                    fio = payload["fio"]
                    ship_store = payload["ship_store"]
                    ship_date = datetime.strptime(payload["ship_date"], "%Y-%m-%d").date()

                    with engine.connect() as conn:
                        for i in idx:
                            r = df.iloc[i]
                            conn.execute(
    text("""
        INSERT INTO archive (uuid, name, article, barcode, quantity, box_num, type, ship_date, fio, ship_store)
        VALUES (:u, :n, :a, :b, :q, :bn, :t, :sd, :fio, :ss)
        ON CONFLICT (uuid) DO UPDATE SET
            name = EXCLUDED.name,
            article = EXCLUDED.article,
            barcode = EXCLUDED.barcode,
            quantity = EXCLUDED.quantity,
            box_num = EXCLUDED.box_num,
            type = EXCLUDED.type,
            ship_date = EXCLUDED.ship_date,
            fio = EXCLUDED.fio,
            ship_store = EXCLUDED.ship_store
    """),
    params_dict
)
                            conn.execute(text("DELETE FROM stock WHERE uuid=:u"), {"u": r["uuid"]})
                        conn.commit()

                    # сброс
                    st.session_state.pop(f"_ship_confirm_{key}", None)
                    st.session_state[f"_open_ship_{key}"] = False
                    reset_selection()
                    st.rerun()

            # УДАЛЕНИЕ: с подтверждением словом
            if c2.button(f"🗑️ Удалить ({len(idx)})", key=f"del_btn_{key}"):
                st.session_state[f"_confirm_delete_{key}"] = True

            if st.session_state.get(f"_confirm_delete_{key}", False):
                st.warning("Удаление необратимо. Введите слово **УДАЛИТЬ** для подтверждения.")
                confirm = st.text_input("Подтверждение", key=f"confirm_{key}")
                if st.button("✅ Подтвердить удаление", key=f"confirm_btn_{key}") and confirm.strip().upper() == "УДАЛИТЬ":
                    with engine.connect() as conn:
                        for i in idx:
                            conn.execute(text("DELETE FROM stock WHERE uuid=:u"), {"u": df.iloc[i]["uuid"]})
                        conn.commit()
                    st.session_state[f"_confirm_delete_{key}"] = False
                    reset_selection()
                    st.rerun()
    else:
        st.info(f"Склад {storage_type} пуст")

with t1:
    render_table("ИП", "ip")

with t2:
    render_table("ООО", "ooo")

# -----------------------------
# Архив
# -----------------------------
with t3:
    arch_type = st.radio("Архив:", ["ИП", "ООО"], horizontal=True, key="arch_sel")

    df_arch = pd.read_sql(text("SELECT * FROM archive WHERE type=:t"), engine, params={"t": arch_type})
    df_arch["type"] = df_arch["type"].replace({"000": "ООО"})
    df_arch = apply_search(df_arch, search)

    if not df_arch.empty:
        view_arch = df_arch[["barcode", "quantity", "box_num", "article", "name", "type", "fio", "ship_store", "ship_date"]].copy()
        view_arch.columns = ["Баркод", "Количество", "Номер короба", "Артикул", "Наименование", "Юр лицо", "ФИО", "Склад отгрузки", "Дата отгрузки"]

        arch_table_key = f"arch_table_{arch_type}_{st.session_state.reset_counter}"
        sel_a = st.dataframe(
            view_arch,
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="multi-row",
            key=arch_table_key,
        )

        # Экспорт всего архива
        output_a = io.BytesIO()
        with pd.ExcelWriter(output_a, engine="xlsxwriter") as writer:
            view_arch.to_excel(writer, index=False, sheet_name="Архив")
        st.download_button(
            f"📥 Скачать архив {arch_type}",
            data=output_a.getvalue(),
            file_name=f"archive_{arch_type}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        idx_a = sel_a.get("selection", {}).get("rows", [])
        if idx_a:
            ca1, ca2 = st.columns(2)

            if ca1.button(f"🔙 Вернуть обратно ({len(idx_a)})", key=f"res_btn_{arch_type}"):
                with engine.connect() as conn:
                    for i in idx_a:
                        uid = df_arch.iloc[i]["uuid"]
                        conn.execute(
                            text(
                                """
                                INSERT INTO stock (uuid, name, article, barcode, quantity, box_num, type)
                                SELECT uuid, name, article, barcode, quantity, box_num, type
                                FROM archive WHERE uuid=:u
                                """
                            ),
                            {"u": uid},
                        )
                        conn.execute(text("DELETE FROM archive WHERE uuid=:u"), {"u": uid})
                    conn.commit()
                reset_selection()
                st.rerun()

            if ca2.button(f"🔥 Очистить ({len(idx_a)})", key=f"clear_btn_{arch_type}"):
                st.warning("Очистка архива необратима. Введите слово **ОЧИСТИТЬ**.")
                confirm2 = st.text_input("Подтверждение очистки", key=f"confirm_clear_{arch_type}")
                if st.button("✅ Подтвердить очистку", key=f"confirm_clear_btn_{arch_type}") and confirm2.strip().upper() == "ОЧИСТИТЬ":
                    with engine.connect() as conn:
                        for i in idx_a:
                            conn.execute(text("DELETE FROM archive WHERE uuid=:u"), {"u": df_arch.iloc[i]["uuid"]})
                        conn.commit()
                    reset_selection()
                    st.rerun()
    else:
        st.info("Архив пуст")

# -----------------------------
# Хранение
# -----------------------------
with t4:
    st.subheader("📦 Текущий расчет (на данный момент)")

    df_now = pd.read_sql(text("SELECT * FROM stock"), engine)
    df_now["type"] = df_now["type"].replace({"000": "ООО"})

    if not df_now.empty:
        b_ip = int((df_now["type"] == "ИП").sum())
        b_ooo = int((df_now["type"] == "ООО").sum())
        p_ip = int(math.ceil(b_ip / 16)) if b_ip else 0
        p_ooo = int(math.ceil(b_ooo / 16)) if b_ooo else 0

        col1, col2, col3 = st.columns(3)
        col1.metric("Коробов (ИП/ООО)", f"{b_ip} / {b_ooo}")
        col2.metric("Паллет всего", p_ip + p_ooo)
        col3.metric("Итого к начислению", f"{(p_ip + p_ooo) * 50} ₽")
    else:
        st.write("Склад пуст")

    st.divider()
    st.subheader("📊 История начислений (архив 23:00)")

    try:
        history_df = pd.read_sql(text("SELECT * FROM daily_storage_logs ORDER BY log_date DESC"), engine)
        if not history_df.empty:
            history_df.columns = ["Дата", "Кор. ИП", "Пал. ИП", "₽ ИП", "Кор. ООО", "Пал. ООО", "₽ ООО", "Итого ₽"]
            st.dataframe(history_df, use_container_width=True, hide_index=True)
        else:
            st.info("История пуста. Первая запись появится после 23:00.")
    except Exception:
        st.warning("Таблица истории (daily_storage_logs) ещё не создана или недоступна.")

# -----------------------------
# Итого
# -----------------------------
with t5:
    df_all = pd.read_sql(text("SELECT * FROM stock"), engine)
    df_all["type"] = df_all["type"].replace({"000": "ООО"})
    if not df_all.empty:
        res = df_all.groupby(["type", "barcode"])["quantity"].sum().reset_index()
        res.columns = ["Юр лицо", "Баркод", "Общее количество"]
        st.dataframe(res, use_container_width=True, hide_index=True)
    else:
        st.info("Нет данных для итога.")
