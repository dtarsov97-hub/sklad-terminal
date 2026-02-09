import streamlit as st
import pandas as pd
import requests
import math
from datetime import datetime, date
import io
import hashlib
from sqlalchemy import create_engine, text

# =========================================================
# СЕКРЕТЫ (Streamlit Cloud -> Manage app -> Settings -> Secrets)
# =========================================================
TOKEN = st.secrets["MS_TOKEN"]
STORE_ID = st.secrets["MS_STORE_ID"]
ORG_ID = st.secrets.get("MS_ORG_ID", "")  # сейчас не используется, оставили на будущее

HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# =========================================================
# БАЗА ДАННЫХ (Neon Postgres или локально SQLite)
# =========================================================
DB_URL = st.secrets.get("DB_URL", "sqlite:///warehouse.db")
engine = create_engine(DB_URL)

# =========================================================
# БД: таблицы stock / archive (archive хранит данные отгрузки)
# =========================================================
def init_db():
try:
    with engine.connect() as conn:
        conn.execute(...)

                CREATE TABLE IF NOT EXISTS stock (
                    uuid TEXT PRIMARY KEY,
                    name TEXT,
                    article TEXT,
                    barcode TEXT,
                    quantity REAL,
                    box_num TEXT,
                    type TEXT
                )
            """))
    
            conn.execute(text("""
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
            """))
    
            # если archive уже существовал (Postgres/Neon) — добавим недостающие столбцы
            try:
                conn.execute(text("ALTER TABLE archive ADD COLUMN IF NOT EXISTS ship_date TEXT"))
                conn.execute(text("ALTER TABLE archive ADD COLUMN IF NOT EXISTS fio TEXT"))
                conn.execute(text("ALTER TABLE archive ADD COLUMN IF NOT EXISTS ship_store TEXT"))
            except Exception:
                pass
    
            conn.commit()
except Exception as e:
    st.error(f"Ошибка при записи в базу: {e}")
    st.stop()

init_db()

# =========================================================
# ЕЖЕДНЕВНЫЙ ЛОГ ХРАНЕНИЯ (23:00+)
# Ожидаем таблицу daily_storage_logs (по вашей инструкции).
# =========================================================
def check_and_log_daily():
    now = datetime.now()
    if now.hour < 23:
        return

    today_str = now.strftime("%Y-%m-%d")
    try:
    with engine.connect() as conn:
            # если таблицы нет — просто выходим
            try:
                res = conn.execute(
                    text("SELECT 1 FROM daily_storage_logs WHERE log_date = :d"),
                    {"d": today_str}
                ).fetchone()
            except Exception:
                return
    
            if res:
                return
    
            df = pd.read_sql(text("SELECT * FROM stock"), engine)
            if df.empty:
                b_ip = b_ooo = 0
            else:
                df["type"] = df["type"].replace({"000": "ООО"})
                b_ip = int((df["type"] == "ИП").sum())
                b_ooo = int((df["type"] == "ООО").sum())
    
            p_ip = int(math.ceil(b_ip / 16)) if b_ip else 0
            p_ooo = int(math.ceil(b_ooo / 16)) if b_ooo else 0
    
            cost_ip = p_ip * 50
            cost_ooo = p_ooo * 50
            total_cost = cost_ip + cost_ooo
    
            conn.execute(text("""
                INSERT INTO daily_storage_logs
                (log_date, boxes_ip, pallets_ip, cost_ip, boxes_ooo, pallets_ooo, cost_ooo, total_cost)
                VALUES (:d, :bi, :pi, :ci, :bo, :po, :co, :tc)
            """), {
                "d": today_str,
                "bi": b_ip, "pi": p_ip, "ci": cost_ip,
                "bo": b_ooo, "po": p_ooo, "co": cost_ooo,
                "tc": total_cost
            })
            conn.commit()
except Exception as e:
    st.error(f"Ошибка при записи в базу: {e}")
    st.stop()

try:
    check_and_log_daily()
except Exception:
    # не валим приложение, если что-то не так с таблицей логов
    pass

# =========================================================
# UI
# =========================================================
st.set_page_config(layout="wide", page_title="Складской Терминал")

if "reset_counter" not in st.session_state:
    st.session_state.reset_counter = 0

def reset_selection():
    st.session_state.reset_counter += 1

# =========================================================
# МОЙСКЛАД (только для подтягивания артикула/названия)
# =========================================================
def load_api_data():
    url = (
        "https://api.moysklad.ru/api/remap/1.2/report/stock/all"
        f"?limit=1000&filter=store=https://api.moysklad.ru/api/remap/1.2/entity/store/{STORE_ID}"
    )
    try:
        res = requests.get(url, headers=HEADERS, timeout=30)
        if res.status_code == 200:
            return res.json().get("rows", [])
        return []
    except Exception:
        return []

ms_rows = load_api_data()
api_status = "🟢 Связь с МойСклад: Установлена" if ms_rows else "🔴 Связь с МойСклад: Ошибка"

st.title("📦 Единая база склада (ИП / ООО)")
st.caption(api_status)

# =========================================================
# Сайдбар: Приемка + Инвентаризация
# =========================================================
with st.sidebar:
    st.header("📥 Приемка")
    uploaded_file = st.file_uploader("Загрузи Excel (Баркод, Кол-во, Короб)", type=["xlsx"])
    target_type = st.radio("Тип поставки:", ["ИП", "ООО"])

    if uploaded_file and st.button("➕ Добавить на баланс"):
        try:
            new_data = pd.read_excel(uploaded_file)
            new_data.columns = ["Баркод", "Кол-во", "Номер короба"]

            mapping = {str(r.get("code")): (r.get("article", "-"), r.get("name", "Неизвестно")) for r in ms_rows}

            try:
    with engine.connect() as conn:
                    for i, row in new_data.iterrows():
                        art, name = mapping.get(str(row["Баркод"]), ("-", "Новый товар"))
                        uid = f"ID_{datetime.now().timestamp()}_{row['Баркод']}_{i}"
                        conn.execute(
                            text("""
                                INSERT INTO stock (uuid, name, article, barcode, quantity, box_num, type)
                                VALUES (:u, :n, :a, :b, :q, :bn, :t)
                            """),
                            {
                                "u": str(uid),
                                "n": str(name),
                                "a": str(art),
                                "b": str(row["Баркод"]),
                                "q": float(row["Кол-во"]),
                                "bn": str(row["Номер короба"]),
                                "t": str(target_type),
                            }
                        )
                    conn.commit()
except Exception as e:
    st.error(f"Ошибка при записи в базу: {e}")
    st.stop()

            reset_selection()
            st.success("Данные сохранены!")
            st.rerun()
        except Exception as e:
            st.error(f"Ошибка: {e}")

    st.divider()
    st.header("📤 Отчёты")

    # Инвентаризация (без удаления)
    try:
        df_all_inv = pd.read_sql(text("SELECT * FROM stock"), engine)
        df_all_inv["type"] = df_all_inv["type"].replace({"000": "ООО"})
    except Exception:
        df_all_inv = pd.DataFrame()

    inv_out = io.BytesIO()
    with pd.ExcelWriter(inv_out, engine="xlsxwriter") as writer:
        if df_all_inv.empty:
            inv_ip = pd.DataFrame(columns=["barcode", "quantity", "box_num", "article", "name", "type"])
            inv_ooo = pd.DataFrame(columns=["barcode", "quantity", "box_num", "article", "name", "type"])
        else:
            inv_ip = df_all_inv[df_all_inv["type"] == "ИП"][["barcode", "quantity", "box_num", "article", "name", "type"]].copy()
            inv_ooo = df_all_inv[df_all_inv["type"] == "ООО"][["barcode", "quantity", "box_num", "article", "name", "type"]].copy()

        for d in (inv_ip, inv_ooo):
            d.columns = ["Баркод", "Количество", "Номер короба", "Артикул", "Наименование", "Юр лицо"]

        inv_ip.to_excel(writer, index=False, sheet_name="ИП")
        inv_ooo.to_excel(writer, index=False, sheet_name="ООО")

    st.download_button(
        "📦 Инвентаризация (скачать остатки)",
        data=inv_out.getvalue(),
        file_name=f"inventory_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# =========================================================
# Поиск + вкладки
# =========================================================
search = st.text_input("🔍 Быстрый поиск (Баркод / Артикул / Короб / Наименование)")

# Хэш поиска нужен, чтобы сбрасывать выделение таблицы при смене фильтра,
# но при этом сохранять "корзину отгрузки" (выбор) между разными поисками.
search_hash = hashlib.md5(search.encode("utf-8")).hexdigest()[:8]

t1, t2, t3, t4, t5 = st.tabs(["🏠 ИП", "🏢 ООО", "📜 Архив", "💰 Хранение", "📊 Итого"])

def apply_search(df: pd.DataFrame, query: str) -> pd.DataFrame:
    if df.empty or not query:
        return df
    q = query.strip()
    mask = df.astype(str).apply(lambda col: col.str.contains(q, case=False, na=False)).any(axis=1)
    return df[mask]


def _norm_str(v):
    """Приводим значения из pandas/numpy к обычным Python-типам для Postgres."""
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if v is None:
        return None
    return str(v)

def make_view_stock(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    d["type"] = d["type"].replace({"000": "ООО"})
    view = d[["barcode", "quantity", "box_num", "article", "name", "type"]].copy()
    view.columns = ["Баркод", "Количество", "Номер короба", "Артикул", "Наименование", "Юр лицо"]
    return view

def make_excel_shipment(selected_rows: pd.DataFrame, storage_type: str, fio: str, ship_store: str, ship_date: date) -> bytes:
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

def upsert_archive_row(conn, r, fio: str, ship_store: str, ship_date: date):
    if hasattr(r, "to_dict"):
        r = r.to_dict()

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
        {
            "u": _norm_str(r.get("uuid")),
            "n": _norm_str(r.get("name")),
            "a": _norm_str(r.get("article")),
            "b": _norm_str(r.get("barcode")),
            "q": float(r.get("quantity") or 0),
            "bn": _norm_str(r.get("box_num")),
            "t": _norm_str(str(r.get("type", "")).replace("000", "ООО")),
            "sd": ship_date.strftime("%d.%m.%Y"),
            "fio": _norm_str(fio),
            "ss": _norm_str(ship_store),
        }
    )



def render_table(storage_type: str, key: str):
    """
    Важно: выбор строк должен сохраняться между разными поисками.
    Поэтому мы используем "корзину" (set uuid) в st.session_state.
    А выделение таблицы сбрасываем при смене поиска через search_hash.
    """
    cart_key = f"ship_cart_{key}"
    if cart_key not in st.session_state:
        st.session_state[cart_key] = set()

    # Загружаем данные по текущему юр.лицу
    df = pd.read_sql(text("SELECT * FROM stock WHERE type=:t"), engine, params={"t": storage_type})
    df["type"] = df["type"].replace({"000": "ООО"})

    # Фильтр поиска
    df_filtered = apply_search(df, search)
    view = make_view_stock(df_filtered)

    if df.empty:
        st.info(f"Склад {storage_type} пуст")
        return

    # Таблица с данными (сброс выделения при смене поиска)
    table_key = f"table_{key}_{st.session_state.reset_counter}_{search_hash}"
    sel = st.dataframe(
        view,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="multi-row",
        key=table_key,
    )
    idx = sel.get("selection", {}).get("rows", [])

    # Кнопки управления корзиной (добавить/очистить)
    cA, cB, cC = st.columns([1, 1, 2])

    if cA.button(f"➕ Добавить в отгрузку ({len(idx)})", disabled=(len(idx) == 0), key=f"add_cart_{key}"):
        # добавляем uuid выбранных строк из ОТФИЛЬТРОВАННОЙ таблицы
        for i in idx:
            st.session_state[cart_key].add(str(df_filtered.iloc[i]["uuid"]))
        st.rerun()

    if cB.button("🧹 Очистить отгрузку", disabled=(len(st.session_state[cart_key]) == 0), key=f"clear_cart_{key}"):
        st.session_state[cart_key] = set()
        st.session_state[f"ship_open_{key}"] = False
        st.rerun()

    # --- Корзина отгрузки (сохранённые строки) ---
    cart_uuids = list(st.session_state[cart_key])
    st.markdown(f"### 🧾 Выбрано к отгрузке: **{len(cart_uuids)}**")

    if cart_uuids:
        # берём строки из общей таблицы df по uuid
        df_cart = df[df["uuid"].astype(str).isin(cart_uuids)].copy()
        # Если что-то уже исчезло из stock (например, отгрузили), чистим корзину
        missing = set(cart_uuids) - set(df_cart["uuid"].astype(str).tolist())
        if missing:
            st.session_state[cart_key] = set(df_cart["uuid"].astype(str).tolist())
            cart_uuids = list(st.session_state[cart_key])

        view_cart = make_view_stock(df_cart)

        cart_table_key = f"cart_table_{key}_{st.session_state.reset_counter}"
        sel_cart = st.dataframe(
            view_cart,
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="multi-row",
            key=cart_table_key,
        )
        cart_idx = sel_cart.get("selection", {}).get("rows", [])

        cc1, cc2 = st.columns(2)
        if cc1.button(f"➖ Убрать из отгрузки ({len(cart_idx)})", disabled=(len(cart_idx) == 0), key=f"rm_cart_{key}"):
            for i in cart_idx:
                st.session_state[cart_key].discard(str(df_cart.iloc[i]["uuid"]))
            st.rerun()

        # --- ОТГРУЗКА ---
        if cc2.button(f"🚀 Отгрузка ({len(cart_uuids)})", disabled=(len(cart_uuids) == 0), key=f"ship_btn_{key}"):
            st.session_state[f"ship_open_{key}"] = True

        if st.session_state.get(f"ship_open_{key}", False):
            if hasattr(st, "dialog"):
                @st.dialog("Параметры отгрузки")
                def ship_dialog():
                    fio = st.text_input("ФИО")
                    ship_store = st.text_input("Склад отгрузки")
                    ship_date = st.date_input("Дата отгрузки", value=datetime.now().date())

                    disabled = not (fio.strip() and ship_store.strip())
                    if disabled:
                        st.info("Заполни ФИО и склад отгрузки, чтобы подтвердить отгрузку.")

                    # актуальные строки из stock
                    df_cart2 = pd.read_sql(
                        text("SELECT * FROM stock WHERE type=:t"),
                        engine,
                        params={"t": storage_type}
                    )
                    df_cart2["type"] = df_cart2["type"].replace({"000": "ООО"})
                    df_cart2 = df_cart2[df_cart2["uuid"].astype(str).isin(list(st.session_state[cart_key]))].copy()

                    excel_bytes = make_excel_shipment(df_cart2, storage_type, fio, ship_store, ship_date)

                    if st.download_button(
                        "⬇️ Скачать и подтвердить отгрузку",
                        data=excel_bytes,
                        file_name=f"shipment_{storage_type}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        disabled=disabled,
                        key=f"dl_ship_{key}_{st.session_state.reset_counter}",
                    ):
                        try:
    with engine.connect() as conn:
                                for _, r in df_cart2.iterrows():
                                    r = r.to_dict()
                                    # гарантируем обычные типы (без numpy)
                                    r["quantity"] = float(r.get("quantity") or 0)
                                    upsert_archive_row(conn, r, fio=fio, ship_store=ship_store, ship_date=ship_date)
                                    conn.execute(text("DELETE FROM stock WHERE uuid=:u"), {"u": r["uuid"]})
                                conn.commit()
except Exception as e:
    st.error(f"Ошибка при записи в базу: {e}")
    st.stop()

                        st.session_state[f"ship_open_{key}"] = False
                        st.session_state[cart_key] = set()
                        reset_selection()
                        st.rerun()

                ship_dialog()
            else:
                with st.expander("Параметры отгрузки", expanded=True):
                    fio = st.text_input("ФИО", key=f"fio_{key}")
                    ship_store = st.text_input("Склад отгрузки", key=f"ship_store_{key}")
                    ship_date = st.date_input("Дата отгрузки", value=datetime.now().date(), key=f"ship_date_{key}")

                    disabled = not (fio.strip() and ship_store.strip())

                    df_cart2 = pd.read_sql(
                        text("SELECT * FROM stock WHERE type=:t"),
                        engine,
                        params={"t": storage_type}
                    )
                    df_cart2["type"] = df_cart2["type"].replace({"000": "ООО"})
                    df_cart2 = df_cart2[df_cart2["uuid"].astype(str).isin(list(st.session_state[cart_key]))].copy()

                    excel_bytes = make_excel_shipment(df_cart2, storage_type, fio, ship_store, ship_date)

                    if st.download_button(
                        "⬇️ Скачать и подтвердить отгрузку",
                        data=excel_bytes,
                        file_name=f"shipment_{storage_type}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        disabled=disabled,
                        key=f"dl_ship_fb_{key}_{st.session_state.reset_counter}",
                    ):
                        with engine.connect() as conn:
                            for _, r in df_cart2.iterrows():
                                r = r.to_dict()
                                r["quantity"] = float(r.get("quantity") or 0)
                                upsert_archive_row(conn, r, fio=fio, ship_store=ship_store, ship_date=ship_date)
                                conn.execute(text("DELETE FROM stock WHERE uuid=:u"), {"u": r["uuid"]})
                            conn.commit()

                        st.session_state[f"ship_open_{key}"] = False
                        st.session_state[cart_key] = set()
                        reset_selection()
                        st.rerun()
    else:
        st.caption("Выбирай строки в таблице сверху и нажимай «Добавить в отгрузку». Потом можешь менять поиск — выбор сохранится.")
with t1:
    render_table("ИП", "ip")

with t2:
    render_table("ООО", "ooo")

# =========================================================
# АРХИВ
# =========================================================
with t3:
    arch_type = st.radio("Архив:", ["ИП", "ООО"], horizontal=True, key="arch_sel")
    df_arch = pd.read_sql(text("SELECT * FROM archive WHERE type=:t"), engine, params={"t": arch_type})
    df_arch["type"] = df_arch["type"].replace({"000": "ООО"})
    df_arch = apply_search(df_arch, search)

    if df_arch.empty:
        st.info("Архив пуст")
    else:
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

        # Скачать архив
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

            if ca1.button(f"🔙 Вернуть обратно ({len(idx_a)})", key=f"arch_restore_{arch_type}"):
                with engine.connect() as conn:
                    for i in idx_a:
                        uid = df_arch.iloc[i]["uuid"]
                        conn.execute(text("""
                            INSERT INTO stock (uuid, name, article, barcode, quantity, box_num, type)
                            SELECT uuid, name, article, barcode, quantity, box_num, type
                            FROM archive WHERE uuid=:u
                            ON CONFLICT (uuid) DO UPDATE SET
                                name = EXCLUDED.name,
                                article = EXCLUDED.article,
                                barcode = EXCLUDED.barcode,
                                quantity = EXCLUDED.quantity,
                                box_num = EXCLUDED.box_num,
                                type = EXCLUDED.type
                        """), {"u": uid})
                        conn.execute(text("DELETE FROM archive WHERE uuid=:u"), {"u": uid})
                    conn.commit()
                reset_selection()
                st.rerun()

            if ca2.button(f"🔥 Удалить из архива ({len(idx_a)})", key=f"arch_delete_{arch_type}"):
                st.session_state[f"arch_del_open_{arch_type}"] = True

            if st.session_state.get(f"arch_del_open_{arch_type}", False):
                st.warning("Очистка необратима. Введите слово **ОЧИСТИТЬ**.")
                confirm2 = st.text_input("Подтверждение очистки", key=f"confirm_clear_{arch_type}")
                if st.button("✅ Подтвердить", key=f"confirm_clear_btn_{arch_type}") and confirm2.strip().upper() == "ОЧИСТИТЬ":
                    with engine.connect() as conn:
                        for i in idx_a:
                            conn.execute(text("DELETE FROM archive WHERE uuid=:u"), {"u": df_arch.iloc[i]["uuid"]})
                        conn.commit()
                    st.session_state[f"arch_del_open_{arch_type}"] = False
                    reset_selection()
                    st.rerun()

# =========================================================
# ХРАНЕНИЕ
# =========================================================
with t4:
    st.subheader("📦 Текущий расчет (на данный момент)")
    df_now = pd.read_sql(text("SELECT * FROM stock"), engine)
    df_now["type"] = df_now["type"].replace({"000": "ООО"})

    if df_now.empty:
        st.write("Склад пуст")
    else:
        b_ip = int((df_now["type"] == "ИП").sum())
        b_ooo = int((df_now["type"] == "ООО").sum())
        p_ip = int(math.ceil(b_ip / 16)) if b_ip else 0
        p_ooo = int(math.ceil(b_ooo / 16)) if b_ooo else 0

        col1, col2, col3 = st.columns(3)
        col1.metric("Коробов (ИП/ООО)", f"{b_ip} / {b_ooo}")
        col2.metric("Паллет всего", p_ip + p_ooo)
        col3.metric("Итого к начислению", f"{(p_ip + p_ooo) * 50} ₽")

    st.divider()
    st.subheader("📊 История начислений (архив 23:00)")

    try:
        history_df = pd.read_sql(text("SELECT * FROM daily_storage_logs ORDER BY log_date DESC"), engine)
        if history_df.empty:
            st.info("История пуста. Первая запись появится после 23:00.")
        else:
            history_df.columns = ["Дата", "Кор. ИП", "Пал. ИП", "₽ ИП", "Кор. ООО", "Пал. ООО", "₽ ООО", "Итого ₽"]
            st.dataframe(history_df, use_container_width=True, hide_index=True)
    except Exception:
        st.warning("Таблица истории (daily_storage_logs) ещё не создана или недоступна.")

# =========================================================
# ИТОГО
# =========================================================
with t5:
    df_all = pd.read_sql(text("SELECT * FROM stock"), engine)
    df_all["type"] = df_all["type"].replace({"000": "ООО"})
    if df_all.empty:
        st.info("Нет данных для итога.")
    else:
        res = df_all.groupby(["type", "barcode"])["quantity"].sum().reset_index()
        res.columns = ["Юр лицо", "Баркод", "Общее количество"]
        st.dataframe(res, use_container_width=True, hide_index=True)
