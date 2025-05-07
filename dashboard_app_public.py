# Copia de dashboard_app.py para versión pública
# Solo incluye lo visible en las imágenes: logo, KPIs, gráficas principales, filtros, exportación y glosario

import dash
from dash import dcc, html, Input, Output, State, callback, ctx, dash_table, no_update
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import os
import numpy as np
from datetime import datetime, date
import logging
import io

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Constantes y Configuración Global ---
FOLDER_RESULTADOS = "resultados_subastas"
FILENAME_PATTERN_AGRUPADOS = "datos_agrupados_SEMANAL_"
FOLDER_EXPORTACION = "graficos_exportados"
DEFAULT_CATEGORIES = ['MC', 'ML']
APP_TITLE = "Dashboard Precios Subastas Ganaderas"
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

def find_latest_file(pattern):
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
    except Exception as e:
        logging.error(f"Error buscando archivo con patrón {pattern}: {e}")
        return None

def load_data(filepath, required_cols):
    if not filepath:
        return pd.DataFrame()
    try:
        df = pd.read_csv(filepath)
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logging.error(f"Faltan columnas esenciales en {filepath}: {missing_cols}.")
            return pd.DataFrame()
        return df
    except Exception as e:
        logging.error(f"Error cargando {filepath}: {e}")
        return pd.DataFrame()

def prepare_aggregated_data(df):
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

def crear_figura_vacia(titulo="No hay datos para mostrar o análisis en proceso..."):
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

# --- Carga Inicial de Datos ---
try:
    os.makedirs(FOLDER_RESULTADOS, exist_ok=True)
    os.makedirs(FOLDER_EXPORTACION, exist_ok=True)
except Exception:
    pass

latest_agg_file = find_latest_file(FILENAME_PATTERN_AGRUPADOS)
df_agg_global_raw = load_data(latest_agg_file, ['FECHA_FERIA', 'Precio_Promedio_Ponderado', 'Sexo', 'Cantidad_Lotes'])
df_agg_global, available_categories, global_min_fecha, global_max_fecha = prepare_aggregated_data(df_agg_global_raw)
last_update_agg = datetime.fromtimestamp(os.path.getmtime(latest_agg_file)).strftime('%d/%m/%Y %H:%M') if latest_agg_file and os.path.exists(latest_agg_file) else 'No disponible'

initial_categories = [cat for cat in DEFAULT_CATEGORIES if cat in available_categories]
if not initial_categories and available_categories:
    initial_categories = [available_categories[0]]

# --- Layout de la Aplicación ---
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUMEN], suppress_callback_exceptions=True)
server = app.server
app.title = APP_TITLE

app.layout = dbc.Container([
    # --- Cabecera con logo y título ---
    dbc.Row([
        dbc.Col([
            html.Img(
                src="/assets/subadatos.png",
                style={"height": "60px", "marginRight": "10px", "maxWidth": "100%"}
            ),
            html.Div("by SUBADATOS", style={"fontWeight": "bold", "fontSize": "1em", "color": "#4B2666", "marginTop": "5px"})
        ], width=12, xs=12, sm=12, md="auto", className="d-flex flex-column align-items-center justify-content-center mb-2"),
        dbc.Col(
            html.H1(APP_TITLE, className="text-center text-primary mt-2 mb-2", style={"fontSize": "1.5em"}),
            width=True
        ),
    ], align="center", className="mb-2 flex-wrap"),
    dcc.Store(id='store-selected-categories', data=initial_categories),
    dcc.Download(id="download-html"),
    dbc.Row(dbc.Col(html.P("Análisis interactivo de precios y volúmenes semanales de subastas ganaderas.",
                           className="text-center text-muted mb-4"), width=12)),
    # --- Fila de Indicadores Clave (KPIs) ---
    dbc.Row(id='kpi-row', children=[
        dbc.Col(dbc.Card(dbc.CardBody(id='kpi-promedio-reciente', children=[html.H5("Precio Prom. Reciente (€/kg)"), html.P("-")])), xs=12, md=4, className="mb-2"),
        dbc.Col(dbc.Card(dbc.CardBody(id='kpi-variacion', children=[html.H5("Variación Semanal"), html.P("-")])), xs=12, md=4, className="mb-2"),
        dbc.Col(dbc.Card(dbc.CardBody(id='kpi-cantidad-lotes', children=[html.H5("Lotes Totales Periodo"), html.P("-")])), xs=12, md=4, className="mb-2"),
    ], className="mb-4 justify-content-center"),
    # --- Fila Principal (Controles y Visualizaciones Originales) ---
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H4("🔍 Controles y Filtros (Gráficos Semanales)", className="text-secondary")),
                dbc.CardBody([
                    dbc.Label('Selecciona Rango de Fechas:', html_for='fecha-picker', className="fw-bold"),
                    dcc.DatePickerRange(
                        id='fecha-picker',
                        min_date_allowed=global_min_fecha, max_date_allowed=global_max_fecha,
                        start_date=global_min_fecha, end_date=global_max_fecha,
                        display_format='DD/MM/YYYY', className="mb-3 d-block",
                        start_date_placeholder_text="Desde", end_date_placeholder_text="Hasta",
                        clearable=False, updatemode='singledate'
                    ),
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
                    dbc.Label("Exportar Gráfico de Precios Semanales:", className="fw-bold"),
                    dbc.Button("Descargar HTML Interactivo", id="btn-export-html", color="success", className="w-100 mb-2"),
                    html.Div(id='export-feedback', className="text-success small mt-2 text-center")
                ])
            ], className="shadow-sm")
        ], xs=12, md=4, lg=3, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Evolución de Precios Semanales (€/kg)", className="text-secondary")),
                dbc.CardBody(dcc.Loading(id="loading-graph-price", children=[dcc.Graph(id='precio-evolucion-graph', style={'height': '40vh', 'width': '100%'})]))
            ], className="mb-4 shadow-sm"),
            dbc.Card([
                 dbc.CardHeader(html.H5("Volumen de Lotes Semanales", className="text-secondary")),
                 dbc.CardBody(dcc.Loading(id="loading-graph-volume", children=[dcc.Graph(id='volumen-evolucion-graph', style={'height': '25vh', 'width': '100%'})]))
            ], className="mb-4 shadow-sm"),
            dbc.Card([
                 dbc.CardHeader(html.H5("Tabla de Datos Semanales Filtrados", className="text-secondary")),
                 dbc.CardBody(dcc.Loading(id="loading-table", children=[
                    dash_table.DataTable(
                        id='tabla-datos-filtrados',
                        columns=[], data=[], page_size=10,
                        style_table={'overflowX': 'auto', 'border': '1px solid #ccc', 'borderRadius': '5px', 'width': '100%'},
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
                        style_as_list_view=True,
                    )
                 ]))
            ], className="shadow-sm")
        ], xs=12, md=8, lg=9)
    ]),
    html.Hr(className="my-4"),
    # --- Modal Glosario ---
    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle("Glosario de Categorías de Ganado")),
        dbc.ModalBody(dbc.ListGroup(
            [dbc.ListGroupItem(f"{code}: {desc}") for code, desc in sorted(CATEGORY_MAP.items())]
        )),
        dbc.ModalFooter(dbc.Button("Cerrar", id="btn-close-glossary", className="ms-auto", n_clicks=0)),
    ], id="modal-glossary", is_open=False, size="lg"),
    html.Div(id='category-tooltips-container', style={'display': 'none'}),
    dbc.Row(dbc.Col(
        html.P([f"Datos Agregados (gráficos semanales) actualizados: {last_update_agg}"],
               className="text-muted text-center mt-5 small")
    , width=12))
], fluid=True)

# --- Callbacks ---
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

@callback(
    Output('download-html', 'data'),
    Output('export-feedback', 'children'),
    Input('btn-export-html', 'n_clicks'),
    State('store-selected-categories', 'data'),
    State('fecha-picker', 'start_date'),
    State('fecha-picker', 'end_date'),
    prevent_initial_call=True
)
def export_price_graph_html(n_clicks, selected_categories, start_date_str, end_date_str):
    if n_clicks is None or n_clicks == 0:
        return no_update, ""
    if df_agg_global.empty:
        return no_update, dbc.Alert("Error: No hay datos agregados cargados para exportar.", color="danger", duration=4000)
    if not selected_categories:
        return no_update, dbc.Alert("Selecciona al menos una categoría para exportar.", color="warning", duration=4000)
    if not start_date_str or not end_date_str:
        return no_update, dbc.Alert("Selecciona un rango de fechas válido.", color="warning", duration=4000)
    try:
        start_date = pd.to_datetime(start_date_str).normalize(); end_date = pd.to_datetime(end_date_str).normalize()
        if start_date > end_date:
            raise ValueError("Fecha inicio > Fecha fin")
    except Exception as e:
        return no_update, dbc.Alert(f"Fechas inválidas para exportar: {e}", color="danger", duration=4000)
    try:
        mask_date_export = (df_agg_global['FECHA_FERIA'] >= start_date) & (df_agg_global['FECHA_FERIA'] <= end_date)
        df_filtered_date_export = df_agg_global[mask_date_export]
        weekly_avg_export = pd.DataFrame()
        if not df_filtered_date_export.empty:
            weekly_avg_export = df_filtered_date_export.groupby(pd.Grouper(key='FECHA_FERIA', freq='W-MON'))['Precio_Promedio_Ponderado'].mean().reset_index()
        mask_categories_export = df_filtered_date_export['Sexo'].isin(selected_categories)
        df_final_export = df_filtered_date_export[mask_categories_export].copy()
        if df_final_export.empty:
            return no_update, dbc.Alert("No hay datos para exportar con los filtros actuales.", color="warning", duration=4000)
        df_final_export['text_label'] = ''
        last_indices_export = df_final_export.loc[df_final_export.groupby('Sexo')['FECHA_FERIA'].idxmax()].index
        df_final_export.loc[last_indices_export, 'text_label'] = df_final_export.loc[last_indices_export, 'Precio_Promedio_Ponderado'].round(0).astype(int).astype(str) + " €"
        fig_export = px.line(df_final_export, x='FECHA_FERIA', y='Precio_Promedio_Ponderado', color='Sexo', text='text_label', title=f'Evolución Semanal Precio ({start_date.strftime("%d/%m/%y")} a {end_date.strftime("%d/%m/%y")})', labels={'FECHA_FERIA': 'Semana', 'Precio_Promedio_Ponderado': 'Precio (€/kg)', 'Sexo': 'Categoría'}, markers=True, template='plotly_white', hover_data=['Cantidad_Lotes'])
        fig_export.update_traces(textposition='top center', textfont=dict(size=10), line=dict(width=2.5), marker=dict(size=6))
        if not weekly_avg_export.empty:
            fig_export.add_trace(go.Scatter(x=weekly_avg_export['FECHA_FERIA'], y=weekly_avg_export['Precio_Promedio_Ponderado'], mode='lines', name='Promedio General', line=dict(dash='dot', color='grey', width=1.5), hoverinfo='skip'))
        fig_export.update_layout(title_x=0.5, legend_title_text='Categorías', xaxis_title="Semana", yaxis_title="Precio (€/kg)", hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_cats = "_".join(filter(str.isalnum, "_".join(selected_categories)))[:30]
        filename = f"grafico_precios_agregados_{safe_cats}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{timestamp}.html"
        buffer = io.StringIO()
        fig_export.write_html(buffer, full_html=True, include_plotlyjs='cdn')
        buffer.seek(0)
        return dcc.send_string(buffer.getvalue(), filename=filename), dbc.Alert(f"¡Descarga iniciada!", color="success", duration=6000)
    except Exception as e:
        return no_update, dbc.Alert(f"Error al exportar: {e}", color="danger", duration=6000)

if __name__ == '__main__':
    if df_agg_global.empty:
        print("------------------------------------------------------------")
        print("¡ERROR CRÍTICO AL INICIAR!")
        print("No se pudieron cargar datos iniciales (agregados).")
        print(f"Verifica que existan archivos CSV válidos en '{FOLDER_RESULTADOS}'")
        print("La aplicación no puede funcionar sin datos.")
        print("------------------------------------------------------------")
    else:
        print("---------------------------------------------------------")
        print("🚀 Iniciando servidor Dash (modo público)...")
        print(f"🔗 Accede a la aplicación en: http://127.0.0.1:8050/")
        print(f"📂 Gráficos exportados se guardarán en: '{FOLDER_EXPORTACION}'")
        print("ℹ️  Presiona CTRL+C para detener el servidor.")
        print("---------------------------------------------------------")
        app.run(debug=True, port=8050, host='127.0.0.1') 