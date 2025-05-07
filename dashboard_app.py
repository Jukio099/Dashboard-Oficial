# dashboard_app.py (Versión 8 - Integración de Análisis Adicionales en Tabs)
import dash
from dash import dcc, html, Input, Output, State, callback, ctx, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots # Necesario para algunos gráficos nuevos
import pandas as pd
import os
import numpy as np
from datetime import datetime, date, timedelta
import logging
import json
import warnings
import re # Importado pero no usado en esta versión final, se puede quitar si se desea

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Constantes y Configuración Global ---
FOLDER_RESULTADOS = "resultados_subastas"
# Archivo para los gráficos originales (agregados semanales)
FILENAME_PATTERN_AGRUPADOS = "datos_agrupados_SEMANAL_"
# Archivo para los nuevos análisis detallados (datos limpios)
FILENAME_PATTERN_LIMPIOS = "datos_limpios_" # Ajusta si el prefijo es diferente
FOLDER_EXPORTACION = "graficos_exportados"
DEFAULT_CATEGORIES = ['MC', 'ML']
APP_TITLE = "Dashboard Precios Subastas Ganaderas"
LEVANTE_CATS = ['ML', 'HL']
GORDO_CATS = ['MC', 'HC']

# Mapa de Categorías (Glosario)
CATEGORY_MAP = {
    'MC': 'Macho de Ceba (Novillo/Toro gordo)',
    'ML': 'Macho de Levante (Ternero/Novillo para engordar)',
    'HL': 'Hembra de Levante (Ternera/Novilla para engordar)',
    'HC': 'Hembra de Ceba (Vaca gorda)',
    'VG': 'Vaca de Cría (Gorda)',
    'VP': 'Vaca de Cría (Preñada)',
    'VS': 'Vaca de Cría (Parida/Seca)',
    'NC': 'Novillo Comercial (Cruce)',
    'NG': 'Novillo Gordo (Similar a MC)',
    'VC': 'Vaca Comercial (Descarte)',
    'XX': 'Lote Mixto (Varias categorías)',
}

# Datos de Festivos (basado en búsqueda previa)
HOLIDAYS_2023 = pd.to_datetime([
    '2023-01-01', '2023-01-09', '2023-03-20', '2023-04-06', '2023-04-07',
    '2023-05-01', '2023-05-22', '2023-06-12', '2023-06-19', '2023-07-03',
    '2023-07-20', '2023-08-07', '2023-08-21', '2023-10-16', '2023-11-06',
    '2023-11-13', '2023-12-08', '2023-12-25'
]).date
HOLIDAYS_2024 = pd.to_datetime([
    '2024-01-01', '2024-01-08', '2024-03-25', '2024-03-28', '2024-03-29',
    '2024-05-01', '2024-05-13', '2024-06-03', '2024-06-10', '2024-07-01',
    '2024-07-20', '2024-08-07', '2024-08-19', '2024-10-14', '2024-11-04',
    '2024-11-11', '2024-12-08', '2024-12-25'
]).date
ALL_HOLIDAYS = np.concatenate([HOLIDAYS_2023, HOLIDAYS_2024])
HOLIDAY_MONDAYS = set(h for h in ALL_HOLIDAYS if pd.Timestamp(h).dayofweek == 0)

# Datos de Precipitación (Resumen basado en búsqueda previa)
PRECIPITATION_INFO = {
    "AnnualAvg_Yopal_mm": 1703,
    "WettestMonth_mm": 256, # May
    "DriestMonth_mm": 25, # Jan
    "RainySeasonMonths": "Abr, May, Jun, Jul",
    "RainySeasonRange_mm": "212-256",
    "Notes": {
        1: "Más seco (~25mm)", 4: "Inicio Lluvias (~212-256mm)", 5: "Más húmedo (~256mm)",
        6: "Lluvioso (~212-256mm)", 7: "Fin Lluvias (~212-256mm)"
    }
}
MONTH_MAP = {1: 'Ene', 2: 'Feb', 3: 'Mar', 4: 'Abr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Ago', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dic'}


# --- Funciones Auxiliares ---

def crear_carpetas_necesarias():
    """Crea las carpetas de resultados y exportación si no existen."""
    try:
        os.makedirs(FOLDER_RESULTADOS, exist_ok=True)
        os.makedirs(FOLDER_EXPORTACION, exist_ok=True)
        logging.info(f"Carpetas '{FOLDER_RESULTADOS}' y '{FOLDER_EXPORTACION}' aseguradas.")
    except OSError as e:
        logging.error(f"Error crítico creando carpetas: {e}. Verifica permisos de escritura.")

def find_latest_file(pattern):
    """Encuentra el archivo más reciente que coincide con el patrón en FOLDER_RESULTADOS."""
    try:
        files = sorted(
            [f for f in os.listdir(FOLDER_RESULTADOS)
             if f.startswith(pattern) and f.endswith(".csv")],
            key=lambda x: os.path.getmtime(os.path.join(FOLDER_RESULTADOS, x)),
            reverse=True
        )
        if not files:
            logging.error(f"No se encontró ningún archivo con el patrón '{pattern}*.csv' en '{FOLDER_RESULTADOS}'.")
            return None
        return os.path.join(FOLDER_RESULTADOS, files[0])
    except FileNotFoundError:
        logging.error(f"Error: La carpeta '{FOLDER_RESULTADOS}' no existe al buscar el patrón {pattern}.")
        return None
    except Exception as e:
        logging.error(f"Error buscando archivo con patrón {pattern}: {e}")
        return None

def load_data(filepath, required_cols):
    """Carga un archivo CSV y realiza validaciones básicas."""
    if not filepath:
        return pd.DataFrame()
    logging.info(f"Cargando archivo de datos: {filepath}")
    try:
        df = pd.read_csv(filepath)
        logging.info(f"Archivo CSV cargado. {df.shape[0]} filas iniciales.")
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logging.error(f"Faltan columnas esenciales en {filepath}: {missing_cols}.")
            return pd.DataFrame()
        return df
    except pd.errors.EmptyDataError:
        logging.error(f"Error: El archivo CSV '{filepath}' está vacío.")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error inesperado cargando {filepath}: {e}", exc_info=True)
        return pd.DataFrame()

def prepare_aggregated_data(df):
    """Prepara los datos agregados semanales para los gráficos originales."""
    if df.empty: return pd.DataFrame(), [], date.today(), date.today()
    try:
        df['FECHA_FERIA'] = pd.to_datetime(df['FECHA_FERIA'], errors='coerce')
        df['Precio_Promedio_Ponderado'] = pd.to_numeric(df['Precio_Promedio_Ponderado'], errors='coerce')
        df['Cantidad_Lotes'] = pd.to_numeric(df['Cantidad_Lotes'], errors='coerce').fillna(0).astype(int)
        df['Sexo'] = df['Sexo'].astype(str).str.strip().str.upper()
        df.dropna(subset=['FECHA_FERIA', 'Precio_Promedio_Ponderado'], inplace=True)
        if df.empty: return pd.DataFrame(), [], date.today(), date.today()
        df = df.sort_values('FECHA_FERIA')
        all_categories = sorted(df['Sexo'].unique())
        min_fecha = df['FECHA_FERIA'].min().date()
        max_fecha = df['FECHA_FERIA'].max().date()
        return df, all_categories, min_fecha, max_fecha
    except Exception as e:
        logging.error(f"Error preparando datos agregados: {e}")
        return pd.DataFrame(), [], date.today(), date.today()

def prepare_clean_data(df):
    """Prepara los datos limpios para los nuevos análisis detallados."""
    if df.empty: return pd.DataFrame()
    logging.info("Preparando datos limpios detallados...")
    try:
        df['FECHA_FERIA'] = pd.to_datetime(df['FECHA_FERIA'], errors='coerce')
        df['HORA_ENTRADA_DT'] = pd.to_datetime(
            df['FECHA_FERIA'].dt.strftime('%Y-%m-%d') + ' ' + df['Hora_Entrada'].astype(str),
            errors='coerce'
        )
        # Usar Precio_Final como Precio_Kg
        df['Precio_Kg'] = pd.to_numeric(df['Precio_Final'], errors='coerce')
        df['Cantidad'] = pd.to_numeric(df['Cantidad'], errors='coerce')
        df['Peso_Promedio'] = pd.to_numeric(df['Peso_Promedio'], errors='coerce')
        df['Sexo'] = df['Sexo'].astype(str).str.strip().str.upper()

        # Feature Extraction
        df['DayOfYear'] = df['FECHA_FERIA'].dt.dayofyear
        df['DayOfWeek'] = df['FECHA_FERIA'].dt.day_name()
        df['DayOfWeekNum'] = df['FECHA_FERIA'].dt.dayofweek
        df['HourOfDay'] = df['HORA_ENTRADA_DT'].dt.hour
        df['Month'] = df['FECHA_FERIA'].dt.month
        df['Year'] = df['FECHA_FERIA'].dt.year

        # Filter invalid data
        df.dropna(subset=['FECHA_FERIA', 'HORA_ENTRADA_DT', 'Precio_Kg', 'Cantidad', 'Peso_Promedio', 'Sexo'], inplace=True)
        df = df[(df['Precio_Kg'] > 500) & (df['Cantidad'] > 0) & (df['Peso_Promedio'] > 0)].copy() # Use copy to avoid SettingWithCopyWarning

        # Holiday Flag
        df['IsTuesdayAfterHolidayMonday'] = df.apply(
            lambda row: row['DayOfWeekNum'] == 1 and (row['FECHA_FERIA'].date() - pd.Timedelta(days=1)) in HOLIDAY_MONDAYS,
            axis=1
        )
        logging.info(f"Preparación datos limpios completa. Shape: {df.shape}. Rango fechas: {df['FECHA_FERIA'].min()} a {df['FECHA_FERIA'].max()}")
        return df
    except Exception as e:
        logging.error(f"Error preparando datos limpios: {e}")
        return pd.DataFrame()

def crear_figura_vacia(titulo="No hay datos para mostrar o análisis en proceso..."):
    """Genera una figura Plotly vacía con un mensaje central."""
    fig = go.Figure()
    fig.update_layout(
        template='plotly_white',
        xaxis={'visible': False},
        yaxis={'visible': False},
        annotations=[{
            "text": titulo, "xref": "paper", "yref": "paper",
            "showarrow": False, "font": {"size": 16}
        }]
    )
    return fig

# --- Funciones de Análisis ---

def analyze_levante_timing(df_clean):
    """Analiza los momentos más económicos para comprar levante."""
    figs = {}
    summary = {}
    df_levante = df_clean[df_clean['Sexo'].isin(LEVANTE_CATS)].copy()
    if df_levante.empty:
        logging.warning("No hay datos de Levante para analizar timing.")
        return figs, summary

    # a) Day of Year
    price_by_doy = df_levante.groupby('DayOfYear')['Precio_Kg'].mean().reset_index()
    if not price_by_doy.empty:
        cheapest_doy_data = price_by_doy.loc[price_by_doy['Precio_Kg'].idxmin()]
        summary['doy'] = f"Día del año ~{cheapest_doy_data['DayOfYear']:.0f} (${cheapest_doy_data['Precio_Kg']:,.0f}/Kg)"
        fig_doy = px.line(price_by_doy, x='DayOfYear', y='Precio_Kg',
                           title='Precio Prom. Levante vs. Día del Año',
                           labels={'DayOfYear': 'Día del Año', 'Precio_Kg': 'Precio Promedio ($/Kg)'})
        fig_doy.add_annotation(x=cheapest_doy_data['DayOfYear'], y=cheapest_doy_data['Precio_Kg'],
                               text=f"Mínimo", showarrow=True, arrowhead=1, ax=-30, ay=-30)
        figs['doy'] = fig_doy
    else: summary['doy'] = "N/A"

    # b) Day of Week (Tue vs Thu)
    price_by_dow_tue_thu = df_levante[df_levante['DayOfWeekNum'].isin([1, 3])]
    if not price_by_dow_tue_thu.empty:
        price_by_dow_grouped = price_by_dow_tue_thu.groupby('DayOfWeek')['Precio_Kg'].mean().reset_index()
        day_order = ["Tuesday", "Thursday"]
        price_by_dow_grouped['DayOfWeek'] = pd.Categorical(price_by_dow_grouped['DayOfWeek'], categories=day_order, ordered=True)
        price_by_dow_grouped = price_by_dow_grouped.sort_values('DayOfWeek')
        summary['dow'] = price_by_dow_grouped.to_dict('records')
        fig_dow = px.bar(price_by_dow_grouped, x='DayOfWeek', y='Precio_Kg',
                          title='Precio Prom. Levante: Martes vs. Jueves',
                          labels={'DayOfWeek': 'Día', 'Precio_Kg': 'Precio Promedio ($/Kg)'}, text_auto='.0f')
        figs['dow'] = fig_dow
    else: summary['dow'] = "N/A"

    # c) Hour of Day
    price_by_hour = df_levante.groupby('HourOfDay')['Precio_Kg'].mean().reset_index()
    if not price_by_hour.empty:
        cheapest_hour_data = price_by_hour.loc[price_by_hour['Precio_Kg'].idxmin()]
        summary['hour'] = f"Hora ~{cheapest_hour_data['HourOfDay']:.0f} (${cheapest_hour_data['Precio_Kg']:,.0f}/Kg)"
        fig_hour = px.line(price_by_hour, x='HourOfDay', y='Precio_Kg', markers=True,
                            title='Precio Prom. Levante vs. Hora de Entrada',
                            labels={'HourOfDay': 'Hora del Día', 'Precio_Kg': 'Precio Promedio ($/Kg)'})
        fig_hour.add_annotation(x=cheapest_hour_data['HourOfDay'], y=cheapest_hour_data['Precio_Kg'],
                                text=f"Mínimo", showarrow=True, arrowhead=1, ax=-30, ay=-30)
        figs['hour'] = fig_hour
    else: summary['hour'] = "N/A"

    return figs, summary

def analyze_lot_size(df_clean):
    """Analiza la relación entre tamaño de lote y precio para levante."""
    figs = {}
    summary = {}
    df_levante = df_clean[df_clean['Sexo'].isin(LEVANTE_CATS)].copy()
    if df_levante.empty:
        logging.warning("No hay datos de Levante para analizar tamaño de lote.")
        return figs, summary

    bins = [0, 5, 10, np.inf]
    labels = ['1-5 Cabezas', '6-10 Cabezas', '>10 Cabezas']
    df_levante['LotSizeBin'] = pd.cut(df_levante['Cantidad'], bins=bins, labels=labels, right=True)
    price_by_lotsize = df_levante.groupby('LotSizeBin')['Precio_Kg'].agg(['mean', 'count']).reset_index()
    summary['bins'] = price_by_lotsize.to_dict('records')

    fig_bar = px.bar(price_by_lotsize, x='LotSizeBin', y='mean', text_auto='.0f', hover_data=['count'],
                     title='Precio Prom. Levante vs. Tamaño del Lote',
                     labels={'LotSizeBin': 'Tamaño del Lote', 'mean': 'Precio Promedio ($/Kg)', 'count': 'Nº Lotes'})
    figs['bar'] = fig_bar

    # Scatter con muestra para rendimiento
    sample_size = min(5000, len(df_levante))
    if sample_size > 0:
        fig_scatter = px.scatter(df_levante.sample(sample_size), x='Cantidad', y='Precio_Kg', opacity=0.5, trendline='ols',
                                title='Precio Levante vs. Cantidad por Lote (Muestra)',
                                labels={'Cantidad': 'Cabezas por Lote', 'Precio_Kg': 'Precio ($/Kg)'})
        figs['scatter'] = fig_scatter
    else:
        figs['scatter'] = crear_figura_vacia("No hay datos suficientes para scatter plot")


    return figs, summary

def analyze_post_holiday(df_clean):
    """Analiza el efecto de los festivos en los precios de ganado gordo."""
    figs = {}
    summary = {}
    df_gordo = df_clean[df_clean['Sexo'].isin(GORDO_CATS)].copy()
    if df_gordo.empty:
        logging.warning("No hay datos de Gordo para analizar post-festivos.")
        return figs, summary

    avg_price_tuesday_after_holiday = df_gordo[df_gordo['IsTuesdayAfterHolidayMonday']]['Precio_Kg'].mean()
    avg_price_other_tuesdays = df_gordo[(df_gordo['DayOfWeekNum'] == 1) & (~df_gordo['IsTuesdayAfterHolidayMonday'])]['Precio_Kg'].mean()
    summary['comparison'] = {
        'Martes Post-Festivo': avg_price_tuesday_after_holiday,
        'Otros Martes': avg_price_other_tuesdays
    }

    # Focus on June 2023 & 2024
    june_holidays_2023 = [pd.Timestamp('2023-06-12').date(), pd.Timestamp('2023-06-19').date()]
    june_holidays_2024 = [pd.Timestamp('2024-06-03').date(), pd.Timestamp('2024-06-10').date()]
    target_tuesdays = [h + pd.Timedelta(days=1) for h in june_holidays_2023 + june_holidays_2024]

    df_gordo_jun = df_gordo[df_gordo['FECHA_FERIA'].dt.month == 6].copy()
    if not df_gordo_jun.empty:
        df_gordo_jun['DateStr'] = df_gordo_jun['FECHA_FERIA'].dt.strftime('%Y-%m-%d')
        price_around_june_holidays = df_gordo_jun.groupby('DateStr')['Precio_Kg'].mean().reset_index().sort_values('DateStr')

        fig_june = px.line(price_around_june_holidays, x='DateStr', y='Precio_Kg', markers=True,
                           title='Precio Prom. Gordo (MC/HC) en Junio (Festivos marcados)',
                           labels={'DateStr': 'Fecha', 'Precio_Kg': 'Precio Promedio ($/Kg)'})
        for t_tue in target_tuesdays:
            t_tue_str = t_tue.strftime('%Y-%m-%d')
            if t_tue_str in price_around_june_holidays['DateStr'].values:
                fig_june.add_vline(x=t_tue_str, line_dash="dash", line_color="red",
                                   annotation_text=f"Martes post-festivo {t_tue.strftime('%b %d')}",
                                   annotation_position="top left")
        figs['june'] = fig_june
    else:
        figs['june'] = crear_figura_vacia("No hay datos de Junio para ganado gordo")

    return figs, summary

def compare_precipitation(df_clean):
    """Genera la tabla comparativa de precios vs. precipitación."""
    summary = {}
    table_data = []
    df_levante = df_clean[df_clean['Sexo'].isin(LEVANTE_CATS)].copy()
    df_gordo = df_clean[df_clean['Sexo'].isin(GORDO_CATS)].copy()

    monthly_avg_price_levante = df_levante.groupby('Month')['Precio_Kg'].mean().to_dict()
    monthly_avg_price_gordo = df_gordo.groupby('Month')['Precio_Kg'].mean().to_dict()

    for month_num in range(1, 13):
        table_data.append({
            'Mes': MONTH_MAP.get(month_num, '?'),
            'Precio Prom. Levante ($/Kg)': f"{monthly_avg_price_levante.get(month_num, 0):,.0f}",
            'Precio Prom. Gordo ($/Kg)': f"{monthly_avg_price_gordo.get(month_num, 0):,.0f}",
            'Precipitación (Notas)': PRECIPITATION_INFO['Notes'].get(month_num, '-')
        })
    summary['table'] = table_data
    summary['annual_avg_precip'] = PRECIPITATION_INFO['AnnualAvg_Yopal_mm']
    # No se generan gráficos por falta de datos mensuales detallados de precipitación
    logging.warning("Generando tabla de comparación de precipitación con datos limitados.")
    return {}, summary # Devuelve figs vacías

# --- Carga Inicial de Datos ---
crear_carpetas_necesarias()

# Cargar datos agregados para gráficos originales
latest_agg_file = find_latest_file(FILENAME_PATTERN_AGRUPADOS)
df_agg_global_raw = load_data(latest_agg_file, ['FECHA_FERIA', 'Precio_Promedio_Ponderado', 'Sexo', 'Cantidad_Lotes'])
df_agg_global, available_categories, global_min_fecha, global_max_fecha = prepare_aggregated_data(df_agg_global_raw)
last_update_agg = datetime.fromtimestamp(os.path.getmtime(latest_agg_file)).strftime('%d/%m/%Y %H:%M') if latest_agg_file and os.path.exists(latest_agg_file) else 'No disponible'

# Cargar datos limpios para nuevos análisis
latest_clean_file = find_latest_file(FILENAME_PATTERN_LIMPIOS)
df_clean_global_raw = load_data(latest_clean_file, ['FECHA_FERIA', 'Hora_Entrada', 'Precio_Final', 'Cantidad', 'Peso_Promedio', 'Sexo'])
df_clean_global = prepare_clean_data(df_clean_global_raw)
last_update_clean = datetime.fromtimestamp(os.path.getmtime(latest_clean_file)).strftime('%d/%m/%Y %H:%M') if latest_clean_file and os.path.exists(latest_clean_file) else 'No disponible'

# Validar categorías iniciales para gráfico original
initial_categories = [cat for cat in DEFAULT_CATEGORIES if cat in available_categories]
if not initial_categories and available_categories:
    initial_categories = [available_categories[0]]
    logging.warning(f"Categorías por defecto no encontradas en datos agregados. Usando: {initial_categories}")
elif len(initial_categories) != len(DEFAULT_CATEGORIES) and available_categories:
     logging.warning(f"Algunas categorías por defecto ({DEFAULT_CATEGORIES}) no están en datos agregados. Usando: {initial_categories}")

# --- Inicializar la Aplicación Dash ---
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUMEN], suppress_callback_exceptions=True)
server = app.server
app.title = APP_TITLE

# --- Layout de la Aplicación ---
app.layout = dbc.Container([
    # --- Cabecera con logo y título ---
    dbc.Row([
        dbc.Col([
            html.Img(
                src="/assets/subadatos.png",  # Ruta relativa a la carpeta assets
                style={"height": "70px", "marginRight": "20px"}
            ),
            html.Div("by SUBADATOS", style={"fontWeight": "bold", "fontSize": "1.1em", "color": "#4B2666", "marginTop": "5px"})
        ], width="auto", className="d-flex flex-column align-items-center justify-content-center"),
        dbc.Col(
            html.H1(APP_TITLE, className="text-center text-primary mt-4 mb-2"),
            width=True
        ),
    ], align="center", className="mb-2"),
    # dcc.Store almacena datos en el navegador del cliente
    dcc.Store(id='store-selected-categories', data=initial_categories),

    # --- Fila de Indicadores Clave (KPIs) ---
    dbc.Row(id='kpi-row', children=[
        dbc.Col(dbc.Card(dbc.CardBody(id='kpi-promedio-reciente', children=[html.H5("Precio Prom. Reciente"), html.P("-")])), md=4, className="mb-2"),
        dbc.Col(dbc.Card(dbc.CardBody(id='kpi-variacion', children=[html.H5("Variación Semanal"), html.P("-")])), md=4, className="mb-2"),
        dbc.Col(dbc.Card(dbc.CardBody(id='kpi-cantidad-lotes', children=[html.H5("Lotes Totales Periodo"), html.P("-")])), md=4, className="mb-2"),
    ], className="mb-4 justify-content-center"),

    # --- Fila Principal (Controles y Visualizaciones Originales) ---
    dbc.Row([
        # --- Columna de Controles ---
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H4("🔍 Controles y Filtros (Gráficos Semanales)", className="text-secondary")),
                dbc.CardBody([
                    # Filtro de Fechas
                    dbc.Label('Selecciona Rango de Fechas:', html_for='fecha-picker', className="fw-bold"),
                    dcc.DatePickerRange(
                        id='fecha-picker',
                        min_date_allowed=global_min_fecha, max_date_allowed=global_max_fecha,
                        start_date=global_min_fecha, end_date=global_max_fecha,
                        display_format='DD/MM/YYYY', className="mb-3 d-block",
                        start_date_placeholder_text="Desde", end_date_placeholder_text="Hasta",
                        clearable=False, updatemode='singledate'
                    ),
                    dbc.Tooltip(f"Datos agregados disponibles: {global_min_fecha.strftime('%d/%m/%Y')} - {global_max_fecha.strftime('%d/%m/%Y')}", target='fecha-picker'),
                    html.Hr(),
                    # Selección de Categorías
                    dbc.Label("Selecciona Categorías (Gráficos Semanales):", html_for='category-selector', className="fw-bold"),
                    dbc.InputGroup([
                        dcc.Dropdown(
                            id='category-selector',
                            options=[{'label': cat, 'value': cat} for cat in available_categories],
                            placeholder="Elige una categoría...", className="flex-grow-1"
                        ),
                        dbc.Button("Añadir (+)", id="btn-add-category", n_clicks=0, color="primary")
                    ], className="mb-2"),
                    dbc.Label("Categorías Seleccionadas:", className="fw-bold mt-2"),
                    dbc.ListGroup(id='list-selected-categories', children=[], className="mb-1", style={"maxHeight": "180px", "overflowY": "auto"}),
                    dbc.ButtonGroup([
                        dbc.Button("Quitar Todas", id="btn-remove-all", color="danger", outline=True, size="sm", n_clicks=0),
                        dbc.Button("Seleccionar Todas", id="btn-select-all", color="secondary", outline=True, size="sm", n_clicks=0)
                    ], size="sm", className="mt-2 d-flex"),
                    dbc.Button("Glosario de Categorías", id="btn-open-glossary", color="info", outline=True, size="sm", className="mt-3 w-100"),
                    html.Hr(),
                    # Exportación
                    dbc.Label("Exportar Gráfico de Precios Semanales:", className="fw-bold"),
                    dbc.Button("Descargar HTML Interactivo", id="btn-export-html", color="success", className="w-100"),
                    dbc.Tooltip("Guarda la gráfica de precios semanales actual como archivo HTML", target="btn-export-html"),
                    html.Div(id='export-feedback', className="text-success small mt-2 text-center")
                ])
            ], className="shadow-sm")
        ], md=4, lg=3, className="mb-4"),

        # --- Columna de Gráficos y Tabla Originales ---
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Evolución de Precios Semanales (€/kg)", className="text-secondary")),
                dbc.CardBody(dcc.Loading(id="loading-graph-price", children=[dcc.Graph(id='precio-evolucion-graph', style={'height': '600px'})])) # Altura aumentada
            ], className="mb-4 shadow-sm"),
            dbc.Card([
                 dbc.CardHeader(html.H5("Volumen de Lotes Semanales", className="text-secondary")),
                 dbc.CardBody(dcc.Loading(id="loading-graph-volume", children=[dcc.Graph(id='volumen-evolucion-graph', style={'height': '30vh'})]))
            ], className="mb-4 shadow-sm"),
            dbc.Card([
                 dbc.CardHeader(html.H5("Tabla de Datos Semanales Filtrados", className="text-secondary")),
                 dbc.CardBody(dcc.Loading(id="loading-table", children=[
                    dash_table.DataTable( # Estilos mejorados aplicados aquí
                        id='tabla-datos-filtrados',
                        columns=[], data=[], page_size=10,
                        style_table={'overflowX': 'auto', 'border': '1px solid #ccc', 'borderRadius': '5px'},
                        style_cell={'padding': '10px 12px', 'textAlign': 'left', 'fontFamily': 'Arial, sans-serif',
                                    'fontSize': '0.95rem', 'borderBottom': '1px solid #eee', 'whiteSpace': 'normal', 'height': 'auto'},
                        style_header={'backgroundColor': '#6c757d', 'color': 'white', 'fontWeight': 'bold',
                                      'textAlign': 'center', 'borderBottom': '2px solid #5a6268'},
                        style_data={'border': 'none', 'borderBottom': '1px solid #eee'},
                        style_data_conditional=[
                            {'if': {'row_index': 'odd'}, 'backgroundColor': 'rgb(248, 248, 248)'},
                            {'if': {'column_id': 'Precio Prom. (€/kg)'}, 'textAlign': 'right'},
                            {'if': {'column_id': 'Cant. Lotes'}, 'textAlign': 'right'},
                            {'if': {'state': 'active'}, 'backgroundColor': 'rgba(0, 116, 217, 0.1)', 'border': '1px solid rgba(0, 116, 217, 0.2)'}
                        ],
                        sort_action="native", filter_action="native", export_format="csv", export_headers="display",
                    )
                 ]))
            ], className="shadow-sm")
        ], md=8, lg=9)
    ]), # Fin Fila Principal Original

    html.Hr(className="my-4"), # Separador

    # --- Sección de Nuevos Análisis (Tabs) ---
    dbc.Row(dbc.Col(html.H2("Análisis Detallados Adicionales", className="text-center text-primary mb-3"), width=12)),
    dbc.Row(dbc.Col([
        dbc.Tabs(id="tabs-analisis", active_tab='tab-levante-tiempo', children=[
            dbc.Tab(label="Tiempo Óptimo (Levante)", tab_id="tab-levante-tiempo", children=[
                dbc.Row([
                    dbc.Col(dcc.Loading(dcc.Graph(id='graph-levante-doy')), md=6),
                    dbc.Col(dcc.Loading(dcc.Graph(id='graph-levante-dow')), md=6),
                ], className="mt-3"),
                dbc.Row([
                     dbc.Col(dcc.Loading(dcc.Graph(id='graph-levante-hour')), md=12),
                ], className="mt-3"),
                dbc.Row(dbc.Col(html.Div(id='summary-levante-tiempo', className="mt-3 p-3 bg-light border rounded")))
            ]),
            dbc.Tab(label="Tamaño Lote (Levante)", tab_id="tab-levante-lote", children=[
                dbc.Row([
                    dbc.Col(dcc.Loading(dcc.Graph(id='graph-levante-lotsize-bar')), md=6),
                    dbc.Col(dcc.Loading(dcc.Graph(id='graph-levante-lotsize-scatter')), md=6),
                ], className="mt-3"),
                 dbc.Row(dbc.Col(html.Div(id='summary-levante-lote', className="mt-3 p-3 bg-light border rounded")))
            ]),
            dbc.Tab(label="Efecto Post-Festivo (Gordo)", tab_id="tab-gordo-festivo", children=[
                 dbc.Row([
                      dbc.Col(dcc.Loading(dcc.Graph(id='graph-gordo-festivo-junio')), md=12),
                 ], className="mt-3"),
                 dbc.Row(dbc.Col(html.Div(id='summary-gordo-festivo', className="mt-3 p-3 bg-light border rounded")))
            ]),
            dbc.Tab(label="Precios vs. Clima (Casanare)", tab_id="tab-clima", children=[
                 dbc.Row(dbc.Col(html.Div(id='summary-clima', className="mt-3 p-3 bg-light border rounded"), width=12)),
                 dbc.Row(dbc.Col(dcc.Loading(dash_table.DataTable(
                     id='table-clima',
                     columns=[], data=[],
                     style_cell={'textAlign': 'left', 'padding': '8px'},
                     style_header={'backgroundColor': 'lightgrey', 'fontWeight': 'bold'}
                 )), width=12), className="mt-3")
            ]),
        ])
    ], width=12)),

    # --- Modal Glosario (Sin cambios) ---
    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle("Glosario de Categorías de Ganado")),
        dbc.ModalBody(dbc.ListGroup(
            [dbc.ListGroupItem(f"{code}: {desc}") for code, desc in sorted(CATEGORY_MAP.items())]
        )),
        dbc.ModalFooter(dbc.Button("Cerrar", id="btn-close-glossary", className="ms-auto", n_clicks=0)),
    ], id="modal-glossary", is_open=False, size="lg"),

    # Contenedor oculto para tooltips (Sin cambios)
    html.Div(id='category-tooltips-container', style={'display': 'none'}),

    # --- Pie de Página ---
    dbc.Row(dbc.Col(
        html.P([f"Datos Agregados (gráficos semanales) actualizados: {last_update_agg}", html.Br(),
                f"Datos Detallados (análisis adicionales) actualizados: {last_update_clean}"],
               className="text-muted text-center mt-5 small")
    , width=12))

], fluid=True)

# --- Callbacks ---

# Callbacks originales (modal, lista categorías, exportación, gráficos principales)
# SIN CAMBIOS SIGNIFICATIVOS EN LA LÓGICA, excepto que usan df_agg_global

@callback(Output("modal-glossary", "is_open"), Input("btn-open-glossary", "n_clicks"), Input("btn-close-glossary", "n_clicks"), State("modal-glossary", "is_open"), prevent_initial_call=True)
def toggle_glossary_modal(n_open, n_close, is_open):
    if ctx.triggered_id in ["btn-open-glossary", "btn-close-glossary"]: return not is_open
    return is_open

@callback(Output('store-selected-categories', 'data'), Output('list-selected-categories', 'children'), Input('btn-add-category', 'n_clicks'), Input({'type': 'remove-category-button', 'index': dash.ALL}, 'n_clicks'), Input('btn-remove-all', 'n_clicks'), Input('btn-select-all', 'n_clicks'), State('category-selector', 'value'), State('store-selected-categories', 'data'))
def update_selected_categories_list(add_clicks, remove_clicks, remove_all_clicks, select_all_clicks, category_to_add, current_selection):
    triggered_id = ctx.triggered_id; current_selection = current_selection or []
    updated_selection = list(current_selection)
    if triggered_id == 'btn-add-category' and category_to_add and category_to_add not in updated_selection: updated_selection.append(category_to_add)
    elif isinstance(triggered_id, dict) and triggered_id.get('type') == 'remove-category-button' and triggered_id['index'] in updated_selection: updated_selection.remove(triggered_id['index'])
    elif triggered_id == 'btn-remove-all' and remove_all_clicks > 0: updated_selection = []
    elif triggered_id == 'btn-select-all' and select_all_clicks > 0: updated_selection = list(available_categories)
    list_items = []
    for category in sorted(updated_selection):
        cat_label_id = f"category-label-{category}"
        list_items.append(dbc.ListGroupItem([html.Span(category, id=cat_label_id, className="me-auto"), dbc.Tooltip(CATEGORY_MAP.get(category, ""), target=cat_label_id, placement='left'), dbc.Button("×", id={'type': 'remove-category-button', 'index': category}, color="danger", outline=True, size="sm", className="ms-2", n_clicks=0)], className="d-flex justify-content-between align-items-center", key=category))
    return updated_selection, list_items

@callback(
    Output('precio-evolucion-graph', 'figure'), Output('volumen-evolucion-graph', 'figure'),
    Output('tabla-datos-filtrados', 'data'), Output('tabla-datos-filtrados', 'columns'),
    Output('kpi-promedio-reciente', 'children'), Output('kpi-variacion', 'children'), Output('kpi-cantidad-lotes', 'children'),
    Input('store-selected-categories', 'data'), Input('fecha-picker', 'start_date'), Input('fecha-picker', 'end_date')
)
def update_main_outputs(selected_categories, start_date_str, end_date_str):
    logging.info(f"Actualizando outputs principales (Agregados). Categorías: {selected_categories}, Fechas: {start_date_str} a {end_date_str}")
    fig_vacia = crear_figura_vacia()
    kpi_defaults = (
        [html.H5("Precio Prom. Reciente (€/kg)", className="card-title"), html.P("-", className="card-text fs-4")],
        [html.H5("Variación Semanal", className="card-title"), html.P("-", className="card-text fs-4")],
        [html.H5("Lotes Totales Periodo", className="card-title"), html.P("-", className="card-text fs-4")]
    )
    if df_agg_global.empty or not start_date_str or not end_date_str:
        return fig_vacia, fig_vacia, [], [], kpi_defaults[0], kpi_defaults[1], kpi_defaults[2]

    try:
        start_date = pd.to_datetime(start_date_str).normalize(); end_date = pd.to_datetime(end_date_str).normalize()
        if start_date > end_date: raise ValueError("Fecha inicio > Fecha fin")
    except Exception as e:
        logging.error(f"Error convirtiendo fechas del DatePickerRange: {e}")
        return fig_vacia, fig_vacia, [], [], kpi_defaults[0], kpi_defaults[1], kpi_defaults[2]

    try:
        mask_date = (df_agg_global['FECHA_FERIA'] >= start_date) & (df_agg_global['FECHA_FERIA'] <= end_date)
        df_filtered_by_date = df_agg_global[mask_date].copy()
        if df_filtered_by_date.empty:
             kpi3_text = [html.H5("Lotes Totales Periodo", className="card-title"), html.H4("0", className="card-text")]
             return fig_vacia, fig_vacia, [], [], kpi_defaults[0], kpi_defaults[1], kpi3_text

        # KPIs
        kpi1_text, kpi2_text, kpi3_text = kpi_defaults
        total_lotes = df_filtered_by_date['Cantidad_Lotes'].sum()
        kpi3_text = [html.H5("Lotes Totales Periodo", className="card-title"), html.H4(f"{total_lotes:,.0f}", className="card-text")]
        weekly_avg = df_filtered_by_date.groupby(pd.Grouper(key='FECHA_FERIA', freq='W-MON'))['Precio_Promedio_Ponderado'].mean().reset_index().sort_values('FECHA_FERIA')
        if not weekly_avg.empty:
            precio_reciente = weekly_avg['Precio_Promedio_Ponderado'].iloc[-1]
            kpi1_text = [html.H5("Precio Prom. Reciente (€/kg)", className="card-title"), html.H4(f"{precio_reciente:,.0f} €", className="card-text")]
            if len(weekly_avg) > 1:
                precio_anterior = weekly_avg['Precio_Promedio_Ponderado'].iloc[-2]
                if precio_anterior and precio_anterior != 0:
                    variacion = ((precio_reciente - precio_anterior) / precio_anterior) * 100; color = "success" if variacion >= 0 else "danger"; signo = "+" if variacion >= 0 else ""
                    kpi2_text = [html.H5("Variación Semanal", className="card-title"), html.H4(f"{signo}{variacion:.1f}%", className=f"card-text text-{color}")]
                else: kpi2_text = [html.H5("Variación Semanal", className="card-title"), html.P("(Ant: 0 o N/A)", className="card-text")]
            else: kpi2_text = [html.H5("Variación Semanal", className="card-title"), html.P("(Semana única)", className="card-text")]

        # Filtrar por Categorías seleccionadas
        if not selected_categories:
            return crear_figura_vacia('Selecciona categorías'), crear_figura_vacia('Selecciona categorías'), [], [], kpi1_text, kpi2_text, kpi3_text
        mask_categories = df_filtered_by_date['Sexo'].isin(selected_categories)
        df_final_filtered = df_filtered_by_date[mask_categories].copy()

        # Gráfico Precios Agregados
        fig_precio = fig_vacia
        if not df_final_filtered.empty:
            df_final_filtered['text_label'] = ''; last_indices = df_final_filtered.loc[df_final_filtered.groupby('Sexo')['FECHA_FERIA'].idxmax()].index
            df_final_filtered.loc[last_indices, 'text_label'] = df_final_filtered.loc[last_indices, 'Precio_Promedio_Ponderado'].round(0).astype(int).astype(str) + " €"
            fig_precio = px.line(df_final_filtered, x='FECHA_FERIA', y='Precio_Promedio_Ponderado', color='Sexo', text='text_label', markers=True, template='plotly_white',
                                labels={'FECHA_FERIA': 'Semana', 'Precio_Promedio_Ponderado': 'Precio Promedio (€/kg)', 'Sexo': 'Categoría'},
                                hover_data={'FECHA_FERIA': '|%d %b %Y', 'Precio_Promedio_Ponderado': ':.0f €', 'Cantidad_Lotes': True, 'Sexo': False, 'text_label': False})
            fig_precio.update_traces(textposition='top center', textfont=dict(size=10), line=dict(width=2.5), marker=dict(size=6))
            fig_precio.update_layout(legend_title_text='Categorías', xaxis_title=None, yaxis_title="Precio (€/kg)", hovermode="x unified", margin=dict(l=40, r=20, t=30, b=30), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
            if not weekly_avg.empty: fig_precio.add_trace(go.Scatter(x=weekly_avg['FECHA_FERIA'], y=weekly_avg['Precio_Promedio_Ponderado'], mode='lines', name='Promedio General', line=dict(dash='dot', color='grey', width=1.5), hoverinfo='skip'))
        else: fig_precio = crear_figura_vacia("No hay datos de precios para la selección")

        # Gráfico Volumen Agregado
        fig_volumen = fig_vacia
        if not df_final_filtered.empty:
            df_volumen = df_final_filtered
            fig_volumen = px.bar(df_volumen, x='FECHA_FERIA', y='Cantidad_Lotes', color='Sexo', barmode='stack', template='plotly_white',
                                labels={'FECHA_FERIA': 'Semana', 'Cantidad_Lotes': 'Cantidad de Lotes', 'Sexo': 'Categoría'},
                                hover_data={'FECHA_FERIA': '|%d %b %Y', 'Cantidad_Lotes': True, 'Sexo': True})
            fig_volumen.update_layout(legend_title_text='Categorías', xaxis_title=None, yaxis_title="Nº Lotes", margin=dict(l=40, r=20, t=30, b=30), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        else: fig_volumen = crear_figura_vacia("No hay datos de volumen para la selección")

        # Tabla Agregada
        table_data, table_columns = [], []
        if not df_final_filtered.empty:
            df_tabla = df_final_filtered[['FECHA_FERIA', 'Sexo', 'Precio_Promedio_Ponderado', 'Cantidad_Lotes']].copy()
            df_tabla['FECHA_FERIA'] = df_tabla['FECHA_FERIA'].dt.strftime('%d/%m/%Y')
            df_tabla['Precio_Promedio_Ponderado'] = df_tabla['Precio_Promedio_Ponderado'].round(0).astype(int)
            df_tabla.rename(columns={'FECHA_FERIA': 'Fecha Semana', 'Sexo': 'Categoría', 'Precio_Promedio_Ponderado': 'Precio Prom. (€/kg)', 'Cantidad_Lotes': 'Cant. Lotes'}, inplace=True)
            table_columns = [{"name": i, "id": i} for i in df_tabla.columns]
            table_data = df_tabla.to_dict('records')

        logging.info("Actualización de outputs principales (Agregados) completada.")
        return fig_precio, fig_volumen, table_data, table_columns, kpi1_text, kpi2_text, kpi3_text

    except Exception as e:
        logging.error(f"Error en callback principal (Agregados): {e}", exc_info=True)
        return fig_vacia, fig_vacia, [], [], kpi_defaults[0], kpi_defaults[1], kpi_defaults[2]


@callback(Output('export-feedback', 'children'), Input('btn-export-html', 'n_clicks'), State('store-selected-categories', 'data'), State('fecha-picker', 'start_date'), State('fecha-picker', 'end_date'), prevent_initial_call=True)
def export_price_graph_html(n_clicks, selected_categories, start_date_str, end_date_str):
    # Lógica de exportación SIN CAMBIOS, usa df_agg_global
    # ... (código de exportación idéntico al de la versión anterior) ...
    # --- Pegar aquí el código de la función export_price_graph_html de la versión 7 ---
    # Asegúrate de que usa df_agg_global en lugar de df_global
    logging.info(f"Intento de exportación HTML (Click: {n_clicks}). Categorías: {selected_categories}, Fechas: {start_date_str}-{end_date_str}")
    if n_clicks is None or n_clicks == 0: return ""
    if df_agg_global.empty: return dbc.Alert("Error: No hay datos agregados cargados para exportar.", color="danger", duration=4000)
    if not selected_categories: return dbc.Alert("Selecciona al menos una categoría para exportar.", color="warning", duration=4000)
    if not start_date_str or not end_date_str: return dbc.Alert("Selecciona un rango de fechas válido.", color="warning", duration=4000)

    try:
        start_date = pd.to_datetime(start_date_str).normalize(); end_date = pd.to_datetime(end_date_str).normalize()
        if start_date > end_date: raise ValueError("Fecha inicio > Fecha fin")
    except Exception as e: return dbc.Alert(f"Fechas inválidas para exportar: {e}", color="danger", duration=4000)

    try:
        mask_date_export = (df_agg_global['FECHA_FERIA'] >= start_date) & (df_agg_global['FECHA_FERIA'] <= end_date)
        df_filtered_date_export = df_agg_global[mask_date_export]
        weekly_avg_export = pd.DataFrame()
        if not df_filtered_date_export.empty: weekly_avg_export = df_filtered_date_export.groupby(pd.Grouper(key='FECHA_FERIA', freq='W-MON'))['Precio_Promedio_Ponderado'].mean().reset_index()
        mask_categories_export = df_filtered_date_export['Sexo'].isin(selected_categories)
        df_final_export = df_filtered_date_export[mask_categories_export].copy()
        if df_final_export.empty: return dbc.Alert("No hay datos para exportar con los filtros actuales.", color="warning", duration=4000)

        df_final_export['text_label'] = ''; last_indices_export = df_final_export.loc[df_final_export.groupby('Sexo')['FECHA_FERIA'].idxmax()].index
        df_final_export.loc[last_indices_export, 'text_label'] = df_final_export.loc[last_indices_export, 'Precio_Promedio_Ponderado'].round(0).astype(int).astype(str) + " €"

        fig_export = px.line(df_final_export, x='FECHA_FERIA', y='Precio_Promedio_Ponderado', color='Sexo', text='text_label', title=f'Evolución Semanal Precio ({start_date.strftime("%d/%m/%y")} a {end_date.strftime("%d/%m/%y")})', labels={'FECHA_FERIA': 'Semana', 'Precio_Promedio_Ponderado': 'Precio (€/kg)', 'Sexo': 'Categoría'}, markers=True, template='plotly_white', hover_data=['Cantidad_Lotes'])
        fig_export.update_traces(textposition='top center', textfont=dict(size=10), line=dict(width=2.5), marker=dict(size=6))
        if not weekly_avg_export.empty: fig_export.add_trace(go.Scatter(x=weekly_avg_export['FECHA_FERIA'], y=weekly_avg_export['Precio_Promedio_Ponderado'], mode='lines', name='Promedio General', line=dict(dash='dot', color='grey', width=1.5), hoverinfo='skip'))
        fig_export.update_layout(title_x=0.5, legend_title_text='Categorías', xaxis_title="Semana", yaxis_title="Precio (€/kg)", hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S"); safe_cats = "_".join(filter(str.isalnum, "_".join(selected_categories)))[:30]
        filename = f"grafico_precios_agregados_{safe_cats}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{timestamp}.html"
        filepath = os.path.join(FOLDER_EXPORTACION, filename)
        fig_export.write_html(filepath, full_html=True, include_plotlyjs='cdn')
        logging.info(f"Gráfico agregado exportado exitosamente a: {filepath}")
        return dbc.Alert(f"¡Éxito! Guardado en: {filepath}", color="success", duration=6000)
    except Exception as e:
        logging.error(f"Error crítico durante la exportación a HTML (agregado): {e}", exc_info=True)
        return dbc.Alert(f"Error al exportar: {e}", color="danger", duration=6000)


# --- NUEVO Callback para Actualizar Tabs de Análisis Detallado ---
@callback(
    # Salidas para la Pestaña 1: Levante Timing
    Output('graph-levante-doy', 'figure'),
    Output('graph-levante-dow', 'figure'),
    Output('graph-levante-hour', 'figure'),
    Output('summary-levante-tiempo', 'children'),
    # Salidas para la Pestaña 2: Levante Lot Size
    Output('graph-levante-lotsize-bar', 'figure'),
    Output('graph-levante-lotsize-scatter', 'figure'),
    Output('summary-levante-lote', 'children'),
    # Salidas para la Pestaña 3: Gordo Post-Holiday
    Output('graph-gordo-festivo-junio', 'figure'),
    Output('summary-gordo-festivo', 'children'),
    # Salidas para la Pestaña 4: Clima
    Output('table-clima', 'data'),
    Output('table-clima', 'columns'),
    Output('summary-clima', 'children'),
    # Input: La pestaña activa
    Input('tabs-analisis', 'active_tab')
)
def render_analysis_tab_content(active_tab):
    """Genera el contenido (gráficos, tablas, resúmenes) para la pestaña de análisis seleccionada."""
    logging.info(f"Actualizando contenido para la pestaña de análisis: {active_tab}")

    # Valores por defecto (figuras vacías y mensajes)
    fig_empty = crear_figura_vacia()
    no_data_msg = html.P("No hay datos detallados disponibles para realizar este análisis.")

    # Inicializar todas las salidas con valores por defecto
    out_levante_doy = fig_empty
    out_levante_dow = fig_empty
    out_levante_hour = fig_empty
    out_summary_levante_tiempo = no_data_msg

    out_levante_lotsize_bar = fig_empty
    out_levante_lotsize_scatter = fig_empty
    out_summary_levante_lote = no_data_msg

    out_gordo_festivo_junio = fig_empty
    out_summary_gordo_festivo = no_data_msg

    out_table_clima_data = []
    out_table_clima_cols = []
    out_summary_clima = no_data_msg

    # Verificar si hay datos detallados disponibles
    if df_clean_global.empty:
        logging.warning("El DataFrame detallado (df_clean_global) está vacío. No se pueden generar análisis adicionales.")
    # Generar contenido basado en la pestaña activa solo si hay datos
    elif active_tab == 'tab-levante-tiempo':
        figs, summary = analyze_levante_timing(df_clean_global)
        out_levante_doy = figs.get('doy', fig_empty)
        out_levante_dow = figs.get('dow', fig_empty)
        out_levante_hour = figs.get('hour', fig_empty)
        summary_text = [
            html.H5("Resultados: Tiempo Óptimo Levante"),
            html.P(f"Día del Año más económico: {summary.get('doy', 'N/A')}"),
            html.P(f"Hora del día más económica: {summary.get('hour', 'N/A')}"),
            html.P(f"Comparativa Martes vs Jueves: Martes=${summary.get('dow', [{'Precio_Kg':0}])[0].get('Precio_Kg',0):,.0f}, Jueves=${summary.get('dow', [{},{'Precio_Kg':0}])[1].get('Precio_Kg',0):,.0f}") if isinstance(summary.get('dow'), list) and len(summary.get('dow',[]))==2 else html.P("Comparativa Martes vs Jueves: N/A")
        ]
        out_summary_levante_tiempo = summary_text

    elif active_tab == 'tab-levante-lote':
        figs, summary = analyze_lot_size(df_clean_global)
        out_levante_lotsize_bar = figs.get('bar', fig_empty)
        out_levante_lotsize_scatter = figs.get('scatter', fig_empty)
        summary_text = [html.H5("Resultados: Tamaño Lote vs. Precio (Levante)")]
        if isinstance(summary.get('bins'), list):
            for item in summary['bins']:
                summary_text.append(html.P(f"Lotes de {item.get('LotSizeBin')}: Precio Promedio ${item.get('mean',0):,.0f} ({item.get('count',0)} lotes)"))
        out_summary_levante_lote = summary_text

    elif active_tab == 'tab-gordo-festivo':
        figs, summary = analyze_post_holiday(df_clean_global)
        out_gordo_festivo_junio = figs.get('june', fig_empty)
        comp = summary.get('comparison', {})
        summary_text = [
            html.H5("Resultados: Efecto Post-Festivo (Gordo)"),
            html.P(f"Precio Prom. Martes Post-Festivo: ${comp.get('Martes Post-Festivo', 0):,.0f}/Kg"),
            html.P(f"Precio Prom. Otros Martes: ${comp.get('Otros Martes', 0):,.0f}/Kg")
        ]
        out_summary_gordo_festivo = summary_text

    elif active_tab == 'tab-clima':
        figs, summary = compare_precipitation(df_clean_global) # figs estará vacío aquí
        table_data = summary.get('table', [])
        if table_data:
            out_table_clima_cols = [{"name": i, "id": i} for i in table_data[0].keys()]
            out_table_clima_data = table_data
        out_summary_clima = html.P(f"Comparación con datos de precipitación limitados. Promedio anual Yopal: {summary.get('annual_avg_precip', 'N/A')} mm.")

    # Devolver todas las salidas
    return (
        out_levante_doy, out_levante_dow, out_levante_hour, out_summary_levante_tiempo,
        out_levante_lotsize_bar, out_levante_lotsize_scatter, out_summary_levante_lote,
        out_gordo_festivo_junio, out_summary_gordo_festivo,
        out_table_clima_data, out_table_clima_cols, out_summary_clima
    )


# --- Ejecución Principal ---
if __name__ == '__main__':
    if df_agg_global.empty and df_clean_global.empty:
        print("------------------------------------------------------------")
        print("¡ERROR CRÍTICO AL INICIAR!")
        print("No se pudieron cargar datos iniciales (ni agregados ni limpios).")
        print(f"Verifica que existan archivos CSV válidos en '{FOLDER_RESULTADOS}'")
        print("La aplicación no puede funcionar sin datos.")
        print("------------------------------------------------------------")
    else:
        if df_agg_global.empty: print("Advertencia: No se cargaron datos agregados, los gráficos semanales no funcionarán.")
        if df_clean_global.empty: print("Advertencia: No se cargaron datos limpios, los análisis detallados no funcionarán.")
        print("---------------------------------------------------------")
        print("🚀 Iniciando servidor Dash (modo desarrollo)...")
        print(f"🔗 Accede a la aplicación en: http://127.0.0.1:8050/")
        print(f"📂 Gráficos exportados se guardarán en: '{FOLDER_EXPORTACION}'")
        print("ℹ️  Presiona CTRL+C para detener el servidor.")
        print("---------------------------------------------------------")
        app.run(debug=True, port=8050, host='127.0.0.1') # Cambiado a app.run