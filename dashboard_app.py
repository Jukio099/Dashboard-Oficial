# dashboard_app.py (Versión Mejorada - V6 - KPIs, Tabla, Volumen, Glosario)
import dash
from dash import dcc, html, Input, Output, State, callback, ctx, dash_table # Importar dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import os
import numpy as np
from datetime import datetime, date, timedelta
import logging
import json

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constantes y Configuración ---
FOLDER_RESULTADOS = "resultados_subastas"
FOLDER_EXPORTACION = "graficos_exportados"
FILENAME_PATTERN_AGRUPADOS = "datos_agrupados_SEMANAL_"
DEFAULT_CATEGORIES = ['MC', 'ML']

# --- MAPA DE CATEGORÍAS (GLOSARIO) ---
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
    # ... (Añadir/corregir según tus datos)
}

# Crear carpetas si no existen
try:
    os.makedirs(FOLDER_RESULTADOS, exist_ok=True)
    os.makedirs(FOLDER_EXPORTACION, exist_ok=True)
except OSError as e:
    logging.error(f"Error creando carpetas: {e}. Asegúrate de permisos.")

# --- Carga de Datos ---
def cargar_datos_semanales():
    """Busca y carga el archivo CSV agrupado semanal más reciente."""
    try:
        filepath = FOLDER_RESULTADOS
        grouped_files = sorted(
            [f for f in os.listdir(filepath) if f.startswith(FILENAME_PATTERN_AGRUPADOS) and f.endswith(".csv")],
            reverse=True
        )
        if not grouped_files:
            logging.error(f"ERROR: No se encontró '{FILENAME_PATTERN_AGRUPADOS}*.csv' en '{filepath}'.")
            return pd.DataFrame(), [], date.today(), date.today()

        data_file = os.path.join(filepath, grouped_files[0])
        logging.info(f"Cargando datos desde: {data_file}")
        df = pd.read_csv(data_file)

        # Procesamiento Inicial
        required_cols = ['FECHA_FERIA', 'Precio_Promedio_Ponderado', 'Sexo']
        if not all(col in df.columns for col in required_cols):
            logging.error(f"ERROR: Faltan columnas esenciales ({required_cols}).")
            return pd.DataFrame(), [], date.today(), date.today()

        df['FECHA_FERIA'] = pd.to_datetime(df['FECHA_FERIA'])
        df = df.sort_values('FECHA_FERIA')
        df['Precio_Promedio_Ponderado'] = pd.to_numeric(df['Precio_Promedio_Ponderado'], errors='coerce')
        # Usar .get con default por si 'Cantidad_Lotes' no existe
        df['Cantidad_Lotes'] = pd.to_numeric(df.get('Cantidad_Lotes', 0), errors='coerce').fillna(0)
        df['Sexo'] = df['Sexo'].astype(str).str.strip()
        df.dropna(subset=['Precio_Promedio_Ponderado'], inplace=True)

        all_categories = sorted(df['Sexo'].unique())
        min_fecha = df['FECHA_FERIA'].min().date()
        max_fecha = df['FECHA_FERIA'].max().date()

        logging.info(f"Datos cargados. {df.shape[0]} filas. Categorías: {all_categories}. Rango Fechas: {min_fecha} a {max_fecha}")
        return df, all_categories, min_fecha, max_fecha

    except Exception as e:
        logging.error(f"Error cargando o procesando datos: {e}")
        return pd.DataFrame(), [], date.today(), date.today()

# Cargar datos al iniciar
df_agrupado, available_sexos, min_fecha, max_fecha = cargar_datos_semanales()

# Verificar categorías por defecto
initial_categories = [cat for cat in DEFAULT_CATEGORIES if cat in available_sexos]
if len(initial_categories) != len(DEFAULT_CATEGORIES):
    logging.warning(f"Categorías por defecto ({DEFAULT_CATEGORIES}) no encontradas. Usando: {initial_categories}")
if not initial_categories and available_sexos:
     initial_categories = [available_sexos[0]]
     logging.warning(f"No hay categorías por defecto válidas. Iniciando con: {initial_categories}")

# --- Inicializar la Aplicación Dash ---
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUMEN])
server = app.server
app.title = "Dashboard Precios Subastas"

# --- Layout de la Aplicación ---
app.layout = dbc.Container([
    dcc.Store(id='store-selected-categories', data=initial_categories),

    # Título Principal y Descripción
    dbc.Row(dbc.Col([
        html.H1("📈 Análisis Semanal de Precios Ganaderos 🐮", className="text-center text-primary mt-4 mb-2"),
        html.P("Visualiza y compara la evolución de precios promedio por kilo (€/kg) para diferentes categorías de ganado.",
               className="text-center text-muted mb-4")
    ], width=12)),

    # --- Fila de KPIs ---
    dbc.Row(id='kpi-row', children=[
        # Los KPIs se llenarán con un callback
        dbc.Col(dbc.Card(dbc.CardBody("...", id='kpi-promedio-reciente')), md=4),
        dbc.Col(dbc.Card(dbc.CardBody("...", id='kpi-variacion')), md=4),
        dbc.Col(dbc.Card(dbc.CardBody("...", id='kpi-cantidad-lotes')), md=4),
    ], className="mb-4"),

    # --- Fila Principal (Filtros y Gráficos/Tabla) ---
    dbc.Row([
        # --- Columna de Controles ---
        dbc.Col([
            # Sección Filtros
            html.Div([
                html.H4("🔍 Filtros de Datos", className="text-secondary mb-3"),
                dbc.Label('Selecciona el Rango de Fechas:', html_for='fecha-picker', id="label-fecha-picker"),
                dbc.Tooltip("Elige el periodo que quieres visualizar.", target="label-fecha-picker", placement='right'),
                dcc.DatePickerRange(
                    id='fecha-picker', min_date_allowed=min_fecha, max_date_allowed=max_fecha,
                    start_date=min_fecha, end_date=max_fecha, display_format='DD/MM/YYYY',
                    className="mb-3 d-block", start_date_placeholder_text="Desde",
                    end_date_placeholder_text="Hasta", clearable=False,
                ),
            ]),
            html.Hr(),

            # Sección Comparador de Categorías
            html.Div([
                html.H5("📊 Comparar Categorías", className="text-secondary mt-4 mb-3"),
                dbc.Label("1. Elige una categoría:", html_for='category-selector', id="label-category-selector"),
                 dbc.Tooltip("Selecciona una categoría de ganado.", target="label-category-selector", placement='right'),
                dbc.InputGroup([
                    dcc.Dropdown(id='category-selector', options=[{'label': i, 'value': i} for i in available_sexos],
                                 placeholder="Seleccionar...", className="flex-grow-1"),
                    dbc.Button("2. Añadir (+)", id="btn-add-category", n_clicks=0, color="primary")
                ], className="mb-3"),
                 dbc.Tooltip("Añade la categoría seleccionada a la gráfica.", target="btn-add-category", placement='right'),

                dbc.Label("Categorías en la gráfica:", id="label-lista-categorias"),
                 dbc.Tooltip("Pasa el ratón sobre un código (ej. MC) para ver qué significa. Clic en '×' para quitar.", target="label-lista-categorias"),
                dbc.ListGroup(id='list-selected-categories', children=[], className="mb-1", style={"maxHeight": "150px", "overflowY": "auto"}), # Altura reducida
                dbc.ButtonGroup([
                    dbc.Button("Quitar Todas", id="btn-remove-all", color="danger", outline=True, size="sm", n_clicks=0),
                    dbc.Button("Seleccionar Todas", id="btn-select-all", color="secondary", outline=True, size="sm", n_clicks=0)
                ], size="sm", className="mt-2 mb-3 w-100"),
                # Botón para Glosario
                dbc.Button("¿Qué significa cada categoría?", id="btn-open-glossary", color="info", outline=True, size="sm", className="w-100")
            ]),
            html.Hr(),

            # Sección Exportación
            html.Div([
                html.H5("💾 Exportar Gráfico", className="text-secondary mt-4 mb-3"),
                dbc.Button("Descargar Gráfico Interactivo (HTML)", id="btn-export-html", color="success", className="w-100"),
                 dbc.Tooltip("Guarda la gráfica de precios actual como un archivo HTML interactivo.", target="btn-export-html", placement='top'),
                html.Div(id='export-feedback', className="text-success mt-2 text-center small")
            ]),

            # Contenedor para Tooltips de Categorías
            html.Div(id='category-tooltips-container', children=[])

        ], md=3, className="bg-light p-4 rounded shadow-sm"), # Fin Columna Controles

        # --- Columna de Gráficos y Tabla ---
        dbc.Col([
            # Gráfico de Precios
            dcc.Loading(id="loading-graph-price", type="circle", children=[
                dcc.Graph(id='precio-evolucion-graph', style={'height': '55vh'}) # Altura ajustada
            ]),
            html.Hr(),
             # Gráfico de Volumen
            dcc.Loading(id="loading-graph-volume", type="circle", children=[
                dcc.Graph(id='volumen-evolucion-graph', style={'height': '25vh'}) # Altura ajustada
            ]),
            html.Hr(),
            # Tabla de Datos
            html.H5("Datos Detallados", className="text-secondary mt-4 mb-2"),
            dcc.Loading(id="loading-table", type="circle", children=[
                dash_table.DataTable(
                    id='tabla-datos-filtrados',
                    columns=[], # Se definen en el callback
                    data=[],    # Se llenan en el callback
                    page_size=10, # Mostrar 10 filas por página
                    style_table={'overflowX': 'auto'}, # Scroll horizontal si es necesario
                    style_cell={'textAlign': 'left', 'padding': '5px'},
                    style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'},
                    sort_action="native", # Habilitar ordenamiento
                    filter_action="native", # Habilitar filtrado básico
                )
            ])
        ], md=9) # Fin Columna Gráficos/Tabla
    ]), # Fin Fila Principal

    # --- Modal para el Glosario ---
    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle("Glosario de Categorías de Ganado")),
        dbc.ModalBody([
            html.Ul([
                html.Li(f"{code}: {desc}") for code, desc in CATEGORY_MAP.items()
            ])
        ]),
        dbc.ModalFooter(
            dbc.Button("Cerrar", id="btn-close-glossary", className="ms-auto", n_clicks=0)
        ),
    ], id="modal-glossary", is_open=False), # Inicialmente cerrado

    dbc.Row(dbc.Col(html.P(f"Datos actualizados desde '{FOLDER_RESULTADOS}'.", className="text-muted text-center mt-5 small"), width=12))
], fluid=True)

# --- Callbacks ---

# Callback para abrir/cerrar el modal del glosario
@callback(
    Output("modal-glossary", "is_open"),
    [Input("btn-open-glossary", "n_clicks"), Input("btn-close-glossary", "n_clicks")],
    [State("modal-glossary", "is_open")],
    prevent_initial_call=True,
)
def toggle_modal(n_open, n_close, is_open):
    if n_open or n_close:
        return not is_open
    return is_open

# Callback para actualizar la lista visual de categorías, el store y los tooltips
@callback(
    Output('store-selected-categories', 'data'),
    Output('list-selected-categories', 'children'),
    Output('category-tooltips-container', 'children'),
    Input('btn-add-category', 'n_clicks'),
    Input({'type': 'remove-category-button', 'index': dash.ALL}, 'n_clicks'),
    Input('btn-remove-all', 'n_clicks'),
    Input('btn-select-all', 'n_clicks'),
    State('category-selector', 'value'),
    State('store-selected-categories', 'data'),
)
def update_selected_categories_list(add_clicks, remove_clicks, remove_all_clicks, select_all_clicks,
                                   category_to_add, current_selection):
    # ... (Lógica idéntica a V5 para añadir/quitar/seleccionar/quitar todas) ...
    triggered_id = ctx.triggered_id
    button_clicked = ctx.triggered_prop_ids

    if current_selection is None: current_selection = []
    updated_selection = list(current_selection) # Crear copia

    # Lógica Botones Quitar/Seleccionar Todas
    if triggered_id == 'btn-remove-all' and remove_all_clicks > 0:
        updated_selection = []
        logging.info("Quitando todas las categorías.")
    elif triggered_id == 'btn-select-all' and select_all_clicks > 0:
        updated_selection = list(available_sexos)
        logging.info("Seleccionando todas las categorías.")
    # Lógica Añadir
    elif triggered_id == 'btn-add-category' and category_to_add:
        if category_to_add not in updated_selection:
            updated_selection.append(category_to_add)
            logging.info(f"Añadiendo: {category_to_add}. Nueva selección: {updated_selection}")
        else: logging.info(f"Categoría {category_to_add} ya existe.")
    # Lógica Quitar Individual
    elif isinstance(triggered_id, dict) and triggered_id['type'] == 'remove-category-button':
         for prop_id, n_clicks_val in button_clicked.items():
            if prop_id.startswith('{"index":') and n_clicks_val is not None and n_clicks_val > 0:
                 try:
                     category_to_remove = json.loads(prop_id.split(".")[0])['index']
                     if category_to_remove in updated_selection:
                         updated_selection.remove(category_to_remove)
                         logging.info(f"Quitando: {category_to_remove}. Nueva selección: {updated_selection}")
                     else: logging.warning(f"Intento de quitar {category_to_remove} que no está en {updated_selection}")
                 except Exception as e: logging.error(f"Error procesando ID de botón para quitar: {prop_id}, Error: {e}")
                 break

    # Generar lista visual y tooltips
    list_items = []
    tooltip_items = []
    for category in sorted(updated_selection):
        category_label_id = f"category-label-{category}"
        list_items.append(
            dbc.ListGroupItem([
                html.Span(category, id=category_label_id),
                dbc.Button("×", id={'type': 'remove-category-button', 'index': category},
                           color="danger", size="sm", className="float-end", n_clicks=0)
            ], className="d-flex justify-content-between align-items-center", key=category)
        )
        if category in CATEGORY_MAP:
            tooltip_items.append(
                dbc.Tooltip(CATEGORY_MAP[category], target=category_label_id, placement='right')
            )

    return updated_selection, list_items, tooltip_items


# Callback PRINCIPAL para actualizar KPIs, Gráficos y Tabla
@callback(
    Output('precio-evolucion-graph', 'figure'),
    Output('volumen-evolucion-graph', 'figure'),
    Output('tabla-datos-filtrados', 'data'),
    Output('tabla-datos-filtrados', 'columns'),
    Output('kpi-promedio-reciente', 'children'),
    Output('kpi-variacion', 'children'),
    Output('kpi-cantidad-lotes', 'children'),
    Input('store-selected-categories', 'data'),
    Input('fecha-picker', 'start_date'),
    Input('fecha-picker', 'end_date'),
)
def update_outputs(selected_categories, start_date, end_date):
    # --- Valores por defecto para todos los outputs ---
    default_fig = go.Figure().update_layout(template='plotly_white', annotations=[{"text": "No hay datos para mostrar", "xref": "paper", "yref": "paper", "showarrow": False}])
    default_table_data = []
    default_table_cols = []
    kpi1_default = [html.H5("Precio Prom. Reciente"), html.P("-")]
    kpi2_default = [html.H5("Variación Semanal"), html.P("-")]
    kpi3_default = [html.H5("Lotes Totales Periodo"), html.P("-")]

    # --- Validaciones iniciales ---
    if df_agrupado.empty:
        logging.warning("No hay datos globales cargados.")
        return default_fig, default_fig, default_table_data, default_table_cols, kpi1_default, kpi2_default, kpi3_default
    if not start_date or not end_date:
        logging.warning("Rango de fechas inválido.")
        return default_fig, default_fig, default_table_data, default_table_cols, kpi1_default, kpi2_default, kpi3_default

    try:
        start_date_dt = pd.to_datetime(start_date)
        end_date_dt = pd.to_datetime(end_date)
        # Asegurar que la fecha de fin incluya todo el día
        end_date_dt = end_date_dt + timedelta(days=1) - timedelta(microseconds=1)
    except ValueError:
        logging.error("Error convirtiendo fechas.")
        return default_fig, default_fig, default_table_data, default_table_cols, kpi1_default, kpi2_default, kpi3_default

    # --- 1. Filtrar por Fecha (Base para KPIs y Gráficos) ---
    mask_date_only = (df_agrupado['FECHA_FERIA'] >= start_date_dt) & (df_agrupado['FECHA_FERIA'] <= end_date_dt)
    df_filtered_by_date = df_agrupado[mask_date_only].copy()

    if df_filtered_by_date.empty:
        logging.warning(f"No hay datos entre {start_date} y {end_date}.")
        kpi3_text = [html.H5("Lotes Totales Periodo"), html.P("0")] # Mostrar 0 lotes
        return default_fig, default_fig, default_table_data, default_table_cols, kpi1_default, kpi2_default, kpi3_text

    # --- 2. Calcular KPIs ---
    # KPI 3: Cantidad Total de Lotes
    total_lotes = df_filtered_by_date['Cantidad_Lotes'].sum()
    kpi3_text = [html.H5("Lotes Totales Periodo"), html.H4(f"{total_lotes:,.0f}")]

    # KPIs 1 y 2: Promedio General Reciente y Variación
    weekly_avg = df_filtered_by_date.groupby('FECHA_FERIA')['Precio_Promedio_Ponderado'].mean().reset_index().sort_values('FECHA_FERIA')
    kpi1_text = kpi1_default
    kpi2_text = kpi2_default
    if not weekly_avg.empty:
        precio_reciente = weekly_avg['Precio_Promedio_Ponderado'].iloc[-1]
        kpi1_text = [html.H5("Precio Prom. Reciente (€/kg)"), html.H4(f"{precio_reciente:,.0f}")]
        if len(weekly_avg) > 1:
            precio_anterior = weekly_avg['Precio_Promedio_Ponderado'].iloc[-2]
            variacion = ((precio_reciente - precio_anterior) / precio_anterior) * 100 if precio_anterior else 0
            color = "success" if variacion >= 0 else "danger"
            signo = "+" if variacion >= 0 else ""
            kpi2_text = [html.H5("Variación Semanal"), html.H4(f"{signo}{variacion:.1f}%", className=f"text-{color}")]
        else:
            kpi2_text = [html.H5("Variación Semanal"), html.P("(semana única)")]

    # --- 3. Filtrar por Categorías Seleccionadas ---
    if not selected_categories:
        logging.warning("No hay categorías seleccionadas.")
        # Mostrar KPIs pero gráficos y tabla vacíos
        fig_precio = go.Figure().update_layout(template='plotly_white', title='Selecciona categorías para ver precios')
        fig_volumen = go.Figure().update_layout(template='plotly_white', title='Selecciona categorías para ver volumen')
        return fig_precio, fig_volumen, default_table_data, default_table_cols, kpi1_text, kpi2_text, kpi3_text

    mask_categories = (df_filtered_by_date['Sexo'].isin(selected_categories))
    filtered_df_final = df_filtered_by_date[mask_categories].copy()

    logging.info(f"Actualizando gráfica y tabla. Categorías: {selected_categories}, Fechas: {start_date} a {end_date}. {filtered_df_final.shape[0]} filas.")

    # --- 4. Generar Gráfico de Precios ---
    fig_precio = default_fig # Valor por defecto
    if not filtered_df_final.empty:
        # Añadir etiquetas al último punto
        filtered_df_final['text_label'] = ''
        last_indices = filtered_df_final.loc[filtered_df_final.groupby('Sexo')['FECHA_FERIA'].idxmax()].index
        filtered_df_final.loc[last_indices, 'text_label'] = filtered_df_final.loc[last_indices, 'Precio_Promedio_Ponderado'].round(0).astype(int).astype(str)

        fig_precio = px.line(filtered_df_final, x='FECHA_FERIA', y='Precio_Promedio_Ponderado', color='Sexo', text='text_label',
                             title='Evolución Semanal del Precio Promedio por Kilo',
                             labels={'FECHA_FERIA': 'Semana', 'Precio_Promedio_Ponderado': 'Precio Promedio (€/kg)', 'Sexo': 'Categoría'},
                             markers=True, template='plotly_white', hover_data=['Cantidad_Lotes'])
        fig_precio.update_traces(textposition='top center', textfont=dict(size=10, color='black'), line=dict(width=2.5))

        # Añadir línea promedio general (calculada antes)
        if not weekly_avg.empty:
            fig_precio.add_trace(go.Scatter(x=weekly_avg['FECHA_FERIA'], y=weekly_avg['Precio_Promedio_Ponderado'], mode='lines',
                                             name='Promedio General', line=dict(dash='dash', color='darkgrey', width=2), hoverinfo='skip'))

        fig_precio.update_layout(title_x=0.5, legend_title_text='Categorías', xaxis_title="Semana", yaxis_title="Precio Promedio (€/kg)",
                                 hovermode="x unified", uniformtext_minsize=8, uniformtext_mode='hide')
    else:
         # Si no hay datos para estas categorías, mostrar mensaje en la gráfica de precios
         fig_precio = go.Figure().update_layout(template='plotly_white', title='Evolución Semanal del Precio Promedio por Kilo',
                                                xaxis_title="Semana", yaxis_title="Precio Promedio (€/kg)",
                                                annotations=[{"text": "No hay datos para las categorías seleccionadas en este periodo.",
                                                              "xref": "paper", "yref": "paper", "showarrow": False, "font": {"size": 14}}])
         # Añadir línea promedio aunque no haya datos de categoría
         if not weekly_avg.empty:
             fig_precio.add_trace(go.Scatter(x=weekly_avg['FECHA_FERIA'], y=weekly_avg['Precio_Promedio_Ponderado'], mode='lines',
                                              name='Promedio General', line=dict(dash='dash', color='darkgrey', width=2), hoverinfo='skip'))


    # --- 5. Generar Gráfico de Volumen ---
    fig_volumen = default_fig # Valor por defecto
    if not filtered_df_final.empty:
        # Agrupar por semana y categoría para sumar lotes
        df_volumen = filtered_df_final.groupby(['FECHA_FERIA', 'Sexo'])['Cantidad_Lotes'].sum().reset_index()
        fig_volumen = px.bar(df_volumen, x='FECHA_FERIA', y='Cantidad_Lotes', color='Sexo',
                             title='Volumen Semanal de Lotes por Categoría',
                             labels={'FECHA_FERIA': 'Semana', 'Cantidad_Lotes': 'Cantidad de Lotes', 'Sexo': 'Categoría'},
                             template='plotly_white', barmode='stack') # Barras apiladas
        fig_volumen.update_layout(title_x=0.5, legend_title_text='Categorías', xaxis_title="Semana", yaxis_title="Cantidad de Lotes")
    else:
         # Si no hay datos para estas categorías, mostrar mensaje en la gráfica de volumen
         fig_volumen = go.Figure().update_layout(template='plotly_white', title='Volumen Semanal de Lotes por Categoría',
                                                 xaxis_title="Semana", yaxis_title="Cantidad de Lotes",
                                                 annotations=[{"text": "No hay datos para mostrar.",
                                                               "xref": "paper", "yref": "paper", "showarrow": False, "font": {"size": 14}}])


    # --- 6. Preparar Datos y Columnas para la Tabla ---
    table_data = default_table_data
    table_cols = default_table_cols
    if not filtered_df_final.empty:
        # Seleccionar y renombrar columnas para la tabla
        df_tabla = filtered_df_final[['FECHA_FERIA', 'Sexo', 'Precio_Promedio_Ponderado', 'Cantidad_Lotes']].copy()
        df_tabla['FECHA_FERIA'] = df_tabla['FECHA_FERIA'].dt.strftime('%Y-%m-%d') # Formatear fecha
        df_tabla.rename(columns={
            'FECHA_FERIA': 'Fecha Semana',
            'Sexo': 'Categoría',
            'Precio_Promedio_Ponderado': 'Precio Prom. (€/kg)',
            'Cantidad_Lotes': 'Cant. Lotes'
        }, inplace=True)
        # Definir columnas para dash_table
        table_cols = [{"name": i, "id": i} for i in df_tabla.columns]
        # Convertir a formato dict para dash_table
        table_data = df_tabla.to_dict('records')

    # --- 7. Devolver todos los outputs ---
    return fig_precio, fig_volumen, table_data, table_cols, kpi1_text, kpi2_text, kpi3_text


# Callback para exportar a HTML (sin cambios funcionales, pero heredará la línea promedio)
@callback(
    Output('export-feedback', 'children'),
    Input('btn-export-html', 'n_clicks'),
    State('store-selected-categories', 'data'),
    State('fecha-picker', 'start_date'),
    State('fecha-picker', 'end_date'),
    prevent_initial_call=True
)
def export_graph_html(n_clicks, selected_categories, start_date, end_date):
    # ... (Lógica idéntica a V5 para validaciones, filtrado, regeneración de figura y guardado) ...
    if n_clicks is None or n_clicks == 0: return ""
    logging.info(f"Botón exportar presionado. Categorías: {selected_categories}, Fechas: {start_date}-{end_date}")
    # (Validaciones y lógica de filtrado/generación de figura idéntica a V5)
    # ...
    if df_agrupado.empty: return "Error: No hay datos cargados."
    if not selected_categories: return "Selecciona al menos una categoría para exportar."
    if not start_date or not end_date: return "Selecciona un rango de fechas válido."

    try:
        start_date_dt = pd.to_datetime(start_date)
        end_date_dt = pd.to_datetime(end_date)
    except ValueError: return "Fechas inválidas seleccionadas para exportar."

    # Re-filtrar sólo por fecha para promedio general
    mask_date_only_export = (
        (df_agrupado['FECHA_FERIA'] >= start_date_dt) &
        (df_agrupado['FECHA_FERIA'] <= end_date_dt)
    )
    df_filtered_by_date_export = df_agrupado[mask_date_only_export].copy()
    weekly_avg_export = pd.DataFrame()
    if not df_filtered_by_date_export.empty:
        weekly_avg_export = df_filtered_by_date_export.groupby('FECHA_FERIA')['Precio_Promedio_Ponderado'].mean().reset_index()

    # Re-filtrar por categorías
    mask_categories_export = (df_filtered_by_date_export['Sexo'].isin(selected_categories))
    filtered_df_export = df_filtered_by_date_export[mask_categories_export].copy()

    if filtered_df_export.empty: return "No hay datos para exportar con los filtros actuales."

    # Añadir etiquetas
    filtered_df_export['text_label'] = ''
    last_indices_export = filtered_df_export.loc[filtered_df_export.groupby('Sexo')['FECHA_FERIA'].idxmax()].index
    filtered_df_export.loc[last_indices_export, 'text_label'] = filtered_df_export.loc[last_indices_export, 'Precio_Promedio_Ponderado'].round(0).astype(int).astype(str)

    # Regenerar figura base
    fig_export = px.line(filtered_df_export, x='FECHA_FERIA', y='Precio_Promedio_Ponderado', color='Sexo', text='text_label',
                         title=f'Evolución Semanal Precio ({start_date} a {end_date})',
                         labels={'FECHA_FERIA': 'Semana', 'Precio_Promedio_Ponderado': 'Precio (€/kg)', 'Sexo': 'Categoría'},
                         markers=True, template='plotly_white', hover_data=['Cantidad_Lotes'])
    fig_export.update_traces(textposition='top center', textfont=dict(size=10, color='black'), line=dict(width=2.5))

    # Añadir línea promedio a la figura exportada
    if not weekly_avg_export.empty:
        fig_export.add_trace(go.Scatter(
            x=weekly_avg_export['FECHA_FERIA'], y=weekly_avg_export['Precio_Promedio_Ponderado'],
            mode='lines', name='Promedio General', line=dict(dash='dash', color='darkgrey', width=2), hoverinfo='skip'
        ))

    fig_export.update_layout(title_x=0.5, legend_title_text='Categorías', xaxis_title="Semana", yaxis_title="Precio (€/kg)", uniformtext_minsize=8, uniformtext_mode='hide')


    # Guardar archivo HTML interactivo
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        safe_sexos_list = ["".join(filter(str.isalnum, str(s))) for s in selected_categories]
        safe_sexos = "_".join(safe_sexos_list) if safe_sexos_list else "ninguna"
    except Exception as e_filename:
        logging.error(f"Error generando nombre de archivo: {e_filename}")
        safe_sexos = "error_nombre"

    filename = f"grafico_precios_{safe_sexos}_{start_date}_a_{end_date}_{timestamp}.html"
    filepath = os.path.join(FOLDER_EXPORTACION, filename)

    try:
        fig_export.write_html(filepath, full_html=True, include_plotlyjs='cdn')
        logging.info(f"Gráfico exportado a: {filepath}")
        return f"¡Éxito! Guardado en: {filepath}"
    except Exception as e:
        logging.error(f"Error al exportar a HTML: {e}")
        return f"Error al guardar: {e}"


# --- Ejecución para Servidor Local (Desarrollo) ---
if __name__ == '__main__':
    if df_agrupado.empty:
        print("---------------------------------------------------------")
        print("ERROR AL INICIAR: No se pudieron cargar los datos.")
        print(f"Verifica CSV semanal válido en '{FOLDER_RESULTADOS}'.")
        print("---------------------------------------------------------")
    else:
        print("---------------------------------------------------------")
        print("Iniciando el servidor Dash...")
        print(f"Accede en: http://127.0.0.1:8050/")
        print(f"Gráficos exportados se guardarán en: '{FOLDER_EXPORTACION}'")
        print("Presiona CTRL+C para detener.")
        print("---------------------------------------------------------")
        app.run(debug=True, port=8050)

