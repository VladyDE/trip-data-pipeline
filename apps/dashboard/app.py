import os
from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

st.set_page_config(
    page_title="Tproject · Satisfacción del Conductor",
    page_icon="🚕",
    layout="wide",
)

CATALOG = os.getenv("CATALOG", "azu")
SCHEMA = os.getenv("SCHEMA", "vladichoffx")
HTTP_PATH = os.getenv("SQL_WAREHOUSE_HTTP_PATH")

TRIPS_TABLE = f"{CATALOG}.{SCHEMA}.trips_obt_gold"
DAILY_TABLE = f"{CATALOG}.{SCHEMA}.daily_metrics_gold"


@st.cache_resource
def get_connection():
    """Abre una conexión al SQL Warehouse usando la identidad OAuth de la app."""
    cfg = Config()  # Toma DATABRICKS_HOST y credenciales del entorno de Databricks Apps
    return sql.connect(
        server_hostname=cfg.host,
        http_path=HTTP_PATH,
        credentials_provider=lambda: cfg.authenticate,
    )


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


def main():
    st.title("🚕 Tproject · Dashboard de Satisfacción del Conductor")
    st.caption("Datos de la capa Gold · `gold_trips_obt` y `gold_metricas_diarias`")

    if not HTTP_PATH:
        st.error(
            "No se encontró `SQL_WAREHOUSE_HTTP_PATH`. Verifica el recurso "
            "`sql-warehouse-http-path` en `app.yaml` y en `databricks.yml`."
        )
        st.stop()

    try:
        min_date, max_date = get_date_bounds()
    except Exception as e:
        st.error(f"No se pudo conectar al warehouse o leer las tablas gold: {e}")
        st.stop()

    # --- Sidebar: filtros interactivos ---
    st.sidebar.header("Filtros")
    date_range = st.sidebar.date_input(
        "Rango de fechas",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if len(date_range) != 2:
        st.info("Selecciona un rango de fechas completo (inicio y fin).")
        st.stop()
    start_date, end_date = date_range

    trips_df = load_trips(start_date, end_date)
    daily_df = load_daily_metrics(start_date, end_date)

    if trips_df.empty:
        st.warning("No hay datos de viajes para el rango seleccionado.")
        st.stop()

    # Filtro adicional por zona de origen, si la columna existe
    zone_col = next((c for c in trips_df.columns if "origen" in c.lower()), None)
    if zone_col:
        zones = sorted(trips_df[zone_col].dropna().unique().tolist())
        selected_zones = st.sidebar.multiselect("Zona de origen", zones, default=zones)
        trips_df = trips_df[trips_df[zone_col].isin(selected_zones)]

    status_col = next((c for c in trips_df.columns if "estado" in c.lower()), None)
    if status_col:
        statuses = sorted(trips_df[status_col].dropna().unique().tolist())
        selected_statuses = st.sidebar.multiselect("Estado", statuses, default=statuses)
        trips_df = trips_df[trips_df[status_col].isin(selected_statuses)]

    # --- KPIs ---
    rating_col = next((c for c in trips_df.columns if "rating" in c.lower()), None)
    revenue_col = next(
        (c for c in trips_df.columns if "costo" in c.lower() or "revenue" in c.lower()),
        None,
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total de viajes", f"{len(trips_df):,}")
    if rating_col:
        col2.metric("Rating promedio", f"{trips_df[rating_col].mean():.2f}")
    if revenue_col:
        col3.metric("Ingresos totales", f"${trips_df[revenue_col].sum():,.2f}")
    if status_col:
        cancel_rate = (trips_df[status_col].astype(str).str.contains(
            "cancel", case=False, na=False
        ).mean() * 100)
        col4.metric("Tasa de cancelación", f"{cancel_rate:.1f}%")

    st.divider()

    # --- Gráficos de métricas diarias ---
    if not daily_df.empty:
        st.subheader("Tendencia diaria")
        left, right = st.columns(2)

        rating_daily_col = next(
            (c for c in daily_df.columns if "rating" in c.lower()), None
        )
        revenue_daily_col = next(
            (c for c in daily_df.columns if "revenue" in c.lower() or "ingreso" in c.lower()),
            None,
        )

        if rating_daily_col:
            fig_rating = px.line(
                daily_df, x="fecha", y=rating_daily_col, markers=True,
                title="Rating promedio por día",
            )
            left.plotly_chart(fig_rating, use_container_width=True)

        if revenue_daily_col:
            fig_revenue = px.bar(
                daily_df, x="fecha", y=revenue_daily_col,
                title="Ingresos por día",
            )
            right.plotly_chart(fig_revenue, use_container_width=True)

        cancel_daily_col = next(
            (c for c in daily_df.columns if "cancel" in c.lower()), None
        )
        if cancel_daily_col:
            fig_cancel = px.line(
                daily_df, x="fecha", y=cancel_daily_col, markers=True,
                title="% de cancelación por día",
            )
            st.plotly_chart(fig_cancel, use_container_width=True)

    st.divider()

    # --- Tabla detallada de viajes ---
    st.subheader("Detalle de viajes")
    st.dataframe(trips_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
