import logging
import os
import sys
from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

# Logs are controled by env variable (so not always on)
DEBUG_MODE = os.getenv("APP_DEBUG", "false").lower() == "true"
LOG_LEVEL = logging.DEBUG if DEBUG_MODE else logging.INFO

logging.basicConfig(
    level=LOG_LEVEL,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("tproject_debug")

if DEBUG_MODE:
    logging.getLogger("databricks.sql").setLevel(logging.DEBUG)
    logging.getLogger("databricks.sdk").setLevel(logging.DEBUG)
else:
    logging.getLogger("databricks.sql").setLevel(logging.WARNING)
    logging.getLogger("databricks.sdk").setLevel(logging.WARNING)

# Page configuration
st.set_page_config(
    page_title="Tproject · Satisfacción del Conductor",
    page_icon="🚕",
    layout="wide",
)

# Define CSS style
st.markdown("""
    <style>
    [data-testid="stMetricValue"] {
        font-size: 28px;
        font-weight: 700;
    }
    </style>
""", unsafe_allow_html=True)

# Provide default values in case something goes wrong with the env variables (might want to change this to ur specific values in ur tenant)
CATALOG = os.getenv("CATALOG", "azu")
SCHEMA = os.getenv("SCHEMA", "vladichoffx")

warehouse_id = os.getenv("SQL_WAREHOUSE_ID")
HTTP_PATH = f"/sql/1.0/warehouses/{warehouse_id}" if warehouse_id else None

# Check which catalog and schema is the app conecting to
logger.info("Config app: CATALOG=%s SCHEMA=%s warehouse_id_set=%s",
            CATALOG, SCHEMA, bool(warehouse_id))

if DEBUG_MODE:
    # White list with harmless env variables
    _safe_debug_keys = {"DATABRICKS_HOST", "SQL_WAREHOUSE_ID", "CATALOG", "SCHEMA"}
    logger.debug("=== DEBUG ENV VARS (modo debug activo) ===")
    for k in sorted(_safe_debug_keys):
        logger.debug("ENV %s=%s", k, os.getenv(k))
    logger.debug("DATABRICKS_CLIENT_ID presente=%s", bool(os.getenv("DATABRICKS_CLIENT_ID")))
    logger.debug("DATABRICKS_CLIENT_SECRET presente=%s", bool(os.getenv("DATABRICKS_CLIENT_SECRET")))
    logger.debug("=== FIN DEBUG ENV VARS ===")

TRIPS_TABLE = f"{CATALOG}.{SCHEMA}.trips_obt_gold"
DAILY_TABLE = f"{CATALOG}.{SCHEMA}.daily_metrics_gold"

# Color Palette (Data Viz Best Practices)
COLOR_PRIMARY = "#2C3E50"
COLOR_REVENUE = "#F0BE4B"
COLOR_RATING = "#17A2B8"
COLOR_CANCEL = "#E74C3C"

@st.cache_resource
def get_connection():
    """Abre una conexión al SQL Warehouse usando la identidad OAuth de la app."""
    logger.debug("get_connection: iniciando")

    if not HTTP_PATH:
        logger.error("get_connection: HTTP_PATH es None/vacío. Verificar SQL_WAREHOUSE_ID.")
        raise ValueError("SQL_WAREHOUSE_ID no está configurado (env var vacía)")

    cfg = Config()

    try:
        cfg.authenticate()
    except Exception:
        logger.exception("get_connection: fallo autenticando contra Databricks")
        raise

    conn = sql.connect(
        server_hostname=cfg.host,
        http_path=HTTP_PATH,
        credentials_provider=lambda: cfg.authenticate,
    )
    logger.info("get_connection: conexión establecida correctamente")
    return conn

@st.cache_data(ttl=300, show_spinner="Consultando datos de viajes...")
def load_trips(start_date: date, end_date: date) -> pd.DataFrame:
    query = f"""
        SELECT *
        FROM {TRIPS_TABLE}
        WHERE fecha_solo BETWEEN ? AND ?
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(query, [start_date, end_date])
        return cur.fetchall_arrow().to_pandas()

@st.cache_data(ttl=300, show_spinner="Consultando métricas diarias...")
def load_daily_metrics(start_date: date, end_date: date) -> pd.DataFrame:
    query = f"""
        SELECT *
        FROM {DAILY_TABLE}
        WHERE fecha_solo BETWEEN ? AND ?
        ORDER BY fecha_solo
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(query, [start_date, end_date])
        return cur.fetchall_arrow().to_pandas()

@st.cache_data(ttl=600)
def get_date_bounds() -> tuple[date, date]:
    query = f"SELECT MIN(fecha_solo) AS min_fecha, MAX(fecha_solo) AS max_fecha FROM {DAILY_TABLE}"
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
        return row.min_fecha, row.max_fecha

# Function to apply same theme in all graphs
def apply_plotly_theme(fig, title="", y_title="", x_title=""):
    fig.update_layout(
        title={
            'text': f"<b>{title}</b>",
            'y': 0.95,
            'x': 0.05,
            'xanchor': 'left',
            'yanchor': 'top',
            'font': dict(size=16, color="#D1DBEB")
        },
        template="plotly_white",
        margin=dict(l=40, r=20, t=60, b=40),
        hovermode="x unified",
        xaxis=dict(showgrid=False, title=x_title),
        yaxis=dict(showgrid=True, gridcolor="#F1F5F9", title=y_title),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig


def main():
    # Header of the app
    st.title("🚕 Información de Viajes Cuenca-Ecuador")
    st.caption("Datos tomados de: `trips_obt_gold` y `daily_metrics_gold`")

    if not HTTP_PATH:
        st.error("No se encontró `SQL_WAREHOUSE_ID`. Verificar el recurso.")
        st.stop()

    try:
        min_date, max_date = get_date_bounds()
    except Exception as e:
        logger.exception("main: error consultando date bounds")
        st.error(f"No se pudo conectar al warehouse o leer las tablas gold: {e}")
        st.stop()

    # Filters of the sidebar
    st.sidebar.markdown("### 🎛️ Filtros de Control")
    date_range = st.sidebar.date_input(
        "Rango de fechas",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if len(date_range) != 2:
        st.info("Por favor, selecciona un rango de fechas completo (inicio y fin).")
        st.stop()
    start_date, end_date = date_range

    trips_df = load_trips(start_date, end_date)
    daily_df = load_daily_metrics(start_date, end_date)

    if trips_df.empty:
        st.warning("No hay datos de viajes para el rango seleccionado.")
        st.stop()

    status_col = next((c for c in trips_df.columns if "estado" in c.lower()), None)
    plate_col = "placa" if "placa" in trips_df.columns else None
    if plate_col:
        plates = sorted(trips_df[plate_col].dropna().unique().tolist())
        selected_plates = st.sidebar.multiselect(
            "Placa",
            plates,
            default=[],
            placeholder="Buscar o seleccionar placa(s)...",
        )
        if selected_plates:
            trips_df = trips_df[trips_df[plate_col].isin(selected_plates)]
    
    if status_col:
        statuses = sorted(trips_df[status_col].dropna().unique().tolist())
        selected_statuses = st.sidebar.multiselect("Estado del viaje", statuses, default=statuses)
        trips_df = trips_df[trips_df[status_col].isin(selected_statuses)]

    # Define KPIS
    rating_col = next((c for c in trips_df.columns if "rating" in c.lower()), None)
    revenue_col = next(
        (c for c in trips_df.columns if "costo" in c.lower() or "revenue" in c.lower()),
        None,
    )

    st.markdown("### 📊 Resumen")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        with st.container(border=True):
            st.metric("Total de Viajes", f"{len(trips_df):,}")
            
    with col2:
        with st.container(border=True):
            if rating_col:
                st.metric("Rating Promedio", f"⭐ {trips_df[rating_col].mean():.2f}")
            else:
                st.metric("Rating Promedio", "N/A")
                
    with col3:
        with st.container(border=True):
            if revenue_col:
                st.metric("Ingresos Totales", f"USD {trips_df[revenue_col].sum():,.2f}")
            else:
                st.metric("Ingresos Totales", "N/A")
                
    with col4:
        with st.container(border=True):
            if status_col:
                cancel_rate = (trips_df[status_col].astype(str).str.contains(
                    "cancel", case=False, na=False
                ).mean() * 100)
                st.metric("Tasa de Cancelación", f"{cancel_rate:.1f}%")
            else:
                st.metric("Tasa de Cancelación", "N/A")

    st.write("")

    # Start ploting the daily metrics graphs with plotly
    if not daily_df.empty:
        st.markdown("### 📈 Tendencias Diarias de Operación")
        
        left, right = st.columns(2)

        rating_daily_col = next((c for c in daily_df.columns if "rating" in c.lower()), None)
        revenue_daily_col = next((c for c in daily_df.columns if "revenue" in c.lower() or "ingreso" in c.lower()), None)

        if rating_daily_col:
            fig_rating = px.line(
                daily_df, x="fecha_solo", y=rating_daily_col,
                markers=True, color_discrete_sequence=[COLOR_RATING]
            )
            apply_plotly_theme(fig_rating, title="Evolución de Satisfacción (Rating)", y_title="Rating", x_title="Fecha")
            left.plotly_chart(fig_rating, use_container_width=True)

        if revenue_daily_col:
            fig_revenue = px.bar(
                daily_df, x="fecha_solo", y=revenue_daily_col,
                color_discrete_sequence=[COLOR_REVENUE]
            )
            apply_plotly_theme(fig_revenue, title="Volumen de Ingresos por Día", y_title="Ingresos ($)", x_title="Fecha")
            right.plotly_chart(fig_revenue, use_container_width=True)

        cancel_daily_col = next((c for c in daily_df.columns if "cancel" in c.lower()), None)
        if cancel_daily_col:
            fig_cancel = px.line(
                daily_df, x="fecha_solo", y=cancel_daily_col,
                markers=True, color_discrete_sequence=[COLOR_CANCEL]
            )
            apply_plotly_theme(fig_cancel, title="Comportamiento de Cancelaciones (%)", y_title="% Cancelado", x_title="Fecha")
            st.plotly_chart(fig_cancel, use_container_width=True)

    # We display the table to enable further analysis if needed
    st.markdown("### 📋 Desglose Detallado de Viajes")
    st.dataframe(
        trips_df, 
        use_container_width=True, 
        hide_index=True
    )

if __name__ == "__main__":
    main()