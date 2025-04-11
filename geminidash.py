import os
import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import dash
# Use AGGrid if available and desired for table display, otherwise stick to standard Dash components
# from dash import dcc, html, Input, Output, State, dash_table, DiskcacheManager, CeleryManager
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import logging
import re # Import regular expressions module

# --- Configuration Section ---
# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
URL_SUBASTA = "https://subastasganaderas.com.co/subastas/"
HEADERS = {'User-Agent': 'Mozilla/5.0'} # Necessary for scraping some sites
FOLDER_RESULTADOS = "resultados_subastas" # Folder to save results
FILENAME_PREFIX_LIMPIOS = "datos_limpios"
FILENAME_PREFIX_AGRUPADOS = "datos_agrupados"

# Create results folder if it doesn't exist
os.makedirs(FOLDER_RESULTADOS, exist_ok=True)
# --- End Configuration Section ---


# --- Data Extraction and Processing Functions ---

def obtener_numeros_subastas():
    """
    Scrapes the main auction page to get the list of available auction numbers.

    Returns:
        list: A list of auction numbers (as strings). Returns empty list on error.
    """
    try:
        response = requests.get(URL_SUBASTA, headers=HEADERS, timeout=20)
        response.raise_for_status() # Raise an exception for bad status codes
        soup = BeautifulSoup(response.text, 'html.parser')
        # Find the select element (dropdown) by its ID
        select_element = soup.find('select', id='subasta_id')
        if not select_element:
            logging.error("Could not find the auction select element (id='subasta_id') on the page.")
            return []
        # Extract auction numbers from the options within the select element
        numeros = [option['value'] for option in select_element.find_all('option') if option.has_attr('value') and option['value']]
        logging.info(f"Found {len(numeros)} auction numbers.")
        # Return numbers sorted numerically (descending) to easily get the latest
        return sorted(numeros, key=int, reverse=True)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching auction list from {URL_SUBASTA}: {e}")
        return []
    except Exception as e:
        logging.error(f"An unexpected error occurred while getting auction numbers: {e}")
        return []

def extraer_datos_subasta(numero_subasta):
    """
    Extracts table data for a specific auction number.

    Args:
        numero_subasta (str or int): The auction number to fetch data for.

    Returns:
        pandas.DataFrame: DataFrame containing the extracted table data,
                          or an empty DataFrame if extraction fails or no table found.
                          Includes 'numero_subasta', 'lugar', and 'fecha' columns.
    """
    url = f"{URL_SUBASTA}{numero_subasta}/"
    logging.info(f"Extracting data for auction {numero_subasta} from {url}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # --- Extract Metadata (Place and Date) ---
        lugar = "Desconocido"
        fecha_str = "Desconocida"
        # Find the div containing metadata - adjust selector if structure changes
        info_div = soup.find('div', class_='subasta_info') # Example selector, needs verification
        if info_div:
             # Example: Extract place using a specific class or structure
            lugar_tag = info_div.find('span', class_='lugar') # Adjust selector
            if lugar_tag:
                lugar = lugar_tag.text.strip()

            # Example: Extract date - might need regex or more specific selectors
            date_text = info_div.text # Or find a specific tag containing the date
            # Try to find a date pattern (example: DD de Month de YYYY)
            match = re.search(r'(\d{1,2}\s+de\s+\w+\s+de\s+\d{4})', date_text)
            if match:
                fecha_str = match.group(1)
                # Optional: Convert to datetime object here if needed immediately
                # month_map = {'enero': '01', 'febrero': '02', ...} # Create mapping if needed
        else:
             # Fallback: Try finding specific elements if the container isn't consistent
             lugar_tag = soup.find('h1') # Example: Assuming place is in H1 like "Subasta 1824 en SUBASTAR SA"
             if lugar_tag:
                 lugar_parts = lugar_tag.text.split(' en ')
                 if len(lugar_parts) > 1:
                     lugar = lugar_parts[-1].strip()
             # Add more specific date finding logic if needed

        logging.info(f"Auction Metadata - Place: {lugar}, Date: {fecha_str}")
        # --- End Extract Metadata ---


        # --- Extract Table Data ---
        tablas = pd.read_html(response.text, attrs={'id': 'tabla_resultados'})

        if tablas:
            df = tablas[0]
            # Add metadata columns
            df['numero_subasta'] = numero_subasta
            df['lugar'] = lugar
            df['fecha_str'] = fecha_str # Store the extracted date string
            logging.info(f"Successfully extracted table for auction {numero_subasta}. Shape: {df.shape}")
            return df
        else:
            logging.warning(f"No table with id='tabla_resultados' found for auction {numero_subasta}.")
            return pd.DataFrame() # Return empty DataFrame if no table found

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data for auction {numero_subasta}: {e}")
        return pd.DataFrame()
    except ValueError as e:
         logging.error(f"Error parsing HTML table for auction {numero_subasta}. Potential issue with table structure or pd.read_html: {e}")
         # Optional: Try bs4 parsing as fallback if pd.read_html fails consistently
         # soup = BeautifulSoup(response.text, 'html.parser')
         # table_element = soup.find('table', id='tabla_resultados')
         # if table_element: ... manually parse rows/cells ...
         return pd.DataFrame()
    except Exception as e:
        logging.error(f"An unexpected error occurred during extraction for auction {numero_subasta}: {e}")
        return pd.DataFrame()


def limpiar_precio_por_kilo(valor):
    """Cleans the 'precio_por_kilo' column."""
    if pd.isna(valor):
        return None
    # Remove '$', '.', spaces, and convert ',' to '.' for decimal conversion
    valor_limpio = str(valor).replace('$', '').replace('.', '').replace(' ', '').replace(',', '.')
    try:
        # Convert to float, handle potential non-numeric values after cleaning
        return float(valor_limpio)
    except ValueError:
        # Log the problematic value if needed
        # logging.warning(f"Could not convert price '{valor}' to float after cleaning. Returning None.")
        return None # Or return 0, or np.nan

def procesar_fecha(fecha_str):
    """Converts the Spanish date string to a datetime object."""
    # Mapping for Spanish month names
    meses = {
        'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
        'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
        'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
    }
    try:
        # Replace " de " and map month name
        for mes_es, mes_num in meses.items():
            if mes_es in fecha_str.lower():
                fecha_str = fecha_str.lower().replace(f" de {mes_es} de ", f"-{mes_num}-")
                break
        # Handle potential day format (e.g., '9' vs '09') and parse
        fecha_dt = pd.to_datetime(fecha_str, format='%d-%m-%Y', dayfirst=True)
        return fecha_dt
    except Exception as e:
        # logging.warning(f"Could not parse date string '{fecha_str}': {e}. Returning NaT.")
        return pd.NaT # Return Not a Time for parsing errors


def agrupar_datos_semanal(df):
    """Groups the cleaned data by week, place, and category."""
    if 'fecha' not in df.columns or df['fecha'].isnull().all():
         logging.error("Cannot group by week, 'fecha' column is missing or all null.")
         return pd.DataFrame() # Return empty if no valid dates

    # Ensure 'precio_por_kilo' and 'cantidad' are numeric, coercing errors
    df['precio_por_kilo'] = pd.to_numeric(df['precio_por_kilo'], errors='coerce')
    df['cantidad'] = pd.to_numeric(df['cantidad'], errors='coerce')

    # Drop rows where essential grouping/aggregation columns are NaN
    df_cleaned = df.dropna(subset=['fecha', 'lugar', 'categoria', 'precio_por_kilo', 'cantidad'])

    if df_cleaned.empty:
        logging.warning("DataFrame is empty after dropping NaNs in essential columns for weekly grouping.")
        return pd.DataFrame()

    # Create 'año_semana' column for grouping (YYYY-WW)
    # df_cleaned['año_semana'] = df_cleaned['fecha'].dt.strftime('%Y-%U') # Sunday as first day
    df_cleaned.loc[:, 'año_semana'] = df_cleaned['fecha'].dt.isocalendar().year.astype(str) + '-' + df_cleaned['fecha'].dt.isocalendar().week.astype(str).str.zfill(2) # ISO week (Monday)


    # Group and aggregate
    df_agrupado = df_cleaned.groupby(['año_semana', 'lugar', 'categoria']).agg(
        precio_promedio_semanal=('precio_por_kilo', 'mean'),
        cantidad_total_semanal=('cantidad', 'sum')
    ).reset_index()

    # Round the average price for better readability
    df_agrupado['precio_promedio_semanal'] = df_agrupado['precio_promedio_semanal'].round(2)

    logging.info("Data successfully grouped by week, place, and category.")
    return df_agrupado


def procesar_datos(df_total):
    """
    Cleans the combined DataFrame and groups data weekly. Saves results to CSV.

    Args:
        df_total (pandas.DataFrame): The DataFrame containing data from multiple auctions.

    Returns:
        tuple: (df_limpio, df_agrupado) DataFrames after cleaning and grouping.
               Returns (empty_df, empty_df) if processing fails.
    """
    if df_total.empty:
        logging.warning("Input DataFrame for processing is empty.")
        return pd.DataFrame(), pd.DataFrame()

    df_limpio = df_total.copy()

    # Rename columns for clarity and consistency (adjust based on actual scraped names)
    # Example: Assuming original columns might be 'Precio $/Kg', 'Fecha Subasta', etc.
    column_mapping = {
        'Precio $/Kg': 'precio_por_kilo', # Adjust source name if different
        'Categoría': 'categoria',       # Adjust source name if different
        'Cantidad': 'cantidad',         # Adjust source name if different
        # 'lugar' and 'fecha_str' are added during extraction
    }
    # Rename only existing columns to avoid errors
    df_limpio.rename(columns={k: v for k, v in column_mapping.items() if k in df_limpio.columns}, inplace=True)

    # --- Data Cleaning ---
    # 1. Clean Price: Apply the cleaning function
    if 'precio_por_kilo' in df_limpio.columns:
        df_limpio['precio_por_kilo'] = df_limpio['precio_por_kilo'].apply(limpiar_precio_por_kilo)
    else:
        logging.warning("'Precio $/Kg' or mapped 'precio_por_kilo' column not found for cleaning.")
        # Optionally create a dummy column with NaNs if needed downstream
        # df_limpio['precio_por_kilo'] = pd.NA

    # 2. Process Date: Convert date string to datetime objects
    if 'fecha_str' in df_limpio.columns:
        df_limpio['fecha'] = df_limpio['fecha_str'].apply(procesar_fecha)
        # Optionally drop the original string column
        # df_limpio = df_limpio.drop(columns=['fecha_str'])
    else:
        logging.warning("'fecha_str' column not found for date processing.")
        df_limpio['fecha'] = pd.NaT # Create if missing

    # 3. Clean Category and Place: Standardize text (lowercase, strip whitespace)
    for col in ['categoria', 'lugar']:
        if col in df_limpio.columns:
            df_limpio[col] = df_limpio[col].astype(str).str.strip().str.lower()
        else:
             logging.warning(f"Column '{col}' not found for cleaning.")


    # 4. Handle Missing Values (Example: fill numeric with 0 or mean, categoric with 'Desconocido')
    if 'cantidad' in df_limpio.columns:
         df_limpio['cantidad'] = pd.to_numeric(df_limpio['cantidad'], errors='coerce').fillna(0)
    # df_limpio['precio_por_kilo'].fillna(df_limpio['precio_por_kilo'].median(), inplace=True) # Example fill
    df_limpio.dropna(subset=['precio_por_kilo', 'fecha', 'lugar', 'categoria'], inplace=True) # Drop rows vital for analysis if NaN

    logging.info(f"Data cleaning complete. Shape after cleaning: {df_limpio.shape}")

    # --- Grouping ---
    df_agrupado = agrupar_datos_semanal(df_limpio)

    # --- Saving Results ---
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        min_subasta = df_limpio['numero_subasta'].min()
        max_subasta = df_limpio['numero_subasta'].max()
        filename_base = f"{min_subasta}_a_{max_subasta}_{timestamp}"

        path_limpios = os.path.join(FOLDER_RESULTADOS, f"{FILENAME_PREFIX_LIMPIOS}_{filename_base}.csv")
        path_agrupados = os.path.join(FOLDER_RESULTADOS, f"{FILENAME_PREFIX_AGRUPADOS}_{filename_base}.csv")

        df_limpio.to_csv(path_limpios, index=False, encoding='utf-8-sig')
        logging.info(f"Cleaned data saved to {path_limpios}")

        if not df_agrupado.empty:
            df_agrupado.to_csv(path_agrupados, index=False, encoding='utf-8-sig')
            logging.info(f"Grouped data saved to {path_agrupados}")
        else:
             logging.warning("Grouped DataFrame is empty, not saving grouped CSV.")

    except Exception as e:
        logging.error(f"Error saving processed data to CSV: {e}")

    return df_limpio, df_agrupado

# --- End Data Extraction and Processing Functions ---


# --- Main Execution and Dash App ---

if __name__ == '__main__':
    logging.info("Starting script execution...")

    # --- Data Fetching ---
    # 1. Get all available auction numbers
    numeros_subastas = obtener_numeros_subastas()

    if not numeros_subastas:
        logging.error("No auction numbers found. Exiting.")
        # Optionally: Try to load the latest existing file if scraping fails
        # list_of_files = glob.glob(os.path.join(FOLDER_RESULTADOS, f"{FILENAME_PREFIX_AGRUPADOS}*.csv"))
        # if list_of_files:
        #     latest_file = max(list_of_files, key=os.path.getctime)
        #     logging.info(f"Loading latest existing file: {latest_file}")
        #     df_final_agrupado = pd.read_csv(latest_file)
        # else:
        #     exit() # Exit if scraping fails and no previous file exists
        exit()


    # 2. Select the last 50 auctions
    #    The list is already sorted descending, so take the first 50
    numeros_subastas_a_extraer = numeros_subastas[:50]
    logging.info(f"Selected the latest 50 auction numbers: {numeros_subastas_a_extraer}")

    # 3. Extract data for selected auctions
    lista_df_subastas = []
    for num in numeros_subastas_a_extraer:
        df_subasta = extraer_datos_subasta(num)
        if not df_subasta.empty:
            lista_df_subastas.append(df_subasta)
        else:
             logging.warning(f"No data extracted for auction {num}. Skipping.")

    if not lista_df_subastas:
         logging.error("Failed to extract data for any of the selected auctions. Exiting.")
         # Again, potentially load latest existing file as fallback
         exit()

    # 4. Combine data into a single DataFrame
    df_total_extraido = pd.concat(lista_df_subastas, ignore_index=True)
    logging.info(f"Combined data from {len(lista_df_subastas)} auctions. Total rows: {df_total_extraido.shape[0]}")

    # --- Data Processing ---
    # 5. Clean and group the extracted data
    df_final_limpio, df_final_agrupado = procesar_datos(df_total_extraido)

    if df_final_agrupado.empty:
        logging.error("Final grouped data is empty after processing. Cannot launch dashboard. Exiting.")
        # Consider loading the latest valid file if this happens
        exit()


    # --- Dashboard Setup ---
    logging.info("Setting up Dash application...")
    # Use Bootstrap themes for better styling
    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUMEN])
    app.title = "Análisis Subastas Ganaderas" # Browser tab title

    # Prepare data for the dashboard
    # Convert 'año_semana' back to a sortable format if needed, or use directly
    # For plotting, convert YYYY-WW to a plottable date (e.g., start of the week)
    try:
         # Use ISO week definition for parsing YYYY-WW
         df_final_agrupado['fecha_inicio_semana'] = pd.to_datetime(df_final_agrupado['año_semana'] + '-1', format='%Y-%W-%w') # %W week starts Monday, %w day 1=Monday
         # Sort data for plotting chronologically
         df_final_agrupado = df_final_agrupado.sort_values('fecha_inicio_semana')
    except Exception as e:
         logging.error(f"Error converting 'año_semana' to 'fecha_inicio_semana': {e}. Plotting might be affected.")
         # Fallback: try sorting by 'año_semana' string directly, may not be perfect
         df_final_agrupado = df_final_agrupado.sort_values('año_semana')


    # Get unique locations for the dropdown
    lugares_disponibles = sorted(df_final_agrupado['lugar'].unique())


    # Define the layout of the dashboard
    app.layout = dbc.Container([
        dbc.Row(dbc.Col(html.H1("Dashboard de Resultados de Subastas Ganaderas", className="text-center text-primary mb-4"), width=12)),

        dbc.Row([
            dbc.Col([
                html.Label("Selecciona el Lugar de la Subasta:"),
                dcc.Dropdown(
                    id='dropdown-lugar',
                    options=[{'label': lugar.title(), 'value': lugar} for lugar in lugares_disponibles], # Capitalize labels
                    value=lugares_disponibles[0] if lugares_disponibles else None, # Default to first location
                    clearable=False,
                    style={'marginBottom': '20px'}
                ),
                # Add a button to export the price chart
                html.Button("Exportar Gráfico de Precios (HTML)", id="btn-export-precio", n_clicks=0, className="btn btn-success me-2"), # Added margin
                 # Add a div to display export confirmation message
                html.Div(id='export-feedback', style={'marginTop': '10px', 'color': 'green'})


            ], md=4), # Column for controls

            dbc.Col([
                dcc.Graph(id='grafico-precio-promedio'),
                dcc.Graph(id='grafico-cantidad-total'),
            ], md=8) # Column for graphs
        ]),

        # Add a section for potential further improvements (optional display)
        dbc.Row(dbc.Col(html.Hr(), width=12)), # Separator
        dbc.Row(dbc.Col(html.H4("Posibles Mejoras:", className="mt-4"), width=12)),
        dbc.Row(dbc.Col([
            html.Ul([
                html.Li("Filtro por Categoría de Ganado."),
                html.Li("Mostrar precio promedio general (además del gráfico)."),
                html.Li("Añadir un Rango de Fechas para filtrar."),
                html.Li("Visualización en Mapa (si hay datos geográficos)."),
                html.Li("Descargar datos filtrados (CSV)."),
                html.Li("Comparar precios entre diferentes lugares o categorías.")
            ])
        ], width=12))

    ], fluid=True) # Use fluid container for full width


    # --- Dashboard Callbacks ---

    # Callback to update graphs based on dropdown selection
    @app.callback(
        [Output('grafico-precio-promedio', 'figure'),
         Output('grafico-cantidad-total', 'figure')],
        [Input('dropdown-lugar', 'value')]
    )
    def update_graph(selected_lugar):
        if not selected_lugar:
            # Return empty figures if no location is selected
             logging.warning("No location selected, returning empty graphs.")
             return px.line(title="Seleccione un lugar"), px.bar(title="Seleccione un lugar")

        logging.info(f"Updating graphs for location: {selected_lugar}")
        df_filtrado = df_final_agrupado[df_final_agrupado['lugar'] == selected_lugar].copy() # Filter data for the chosen location

        if df_filtrado.empty:
             logging.warning(f"No data found for selected location: {selected_lugar}")
             return px.line(title=f"No hay datos para {selected_lugar.title()}"), px.bar(title=f"No hay datos para {selected_lugar.title()}")


        # --- Create Price Trend Figure ---
        # Ensure data is sorted by date for line chart
        df_filtrado = df_filtrado.sort_values('fecha_inicio_semana')

        # Create the figure for Average Price per Kilo
        fig_precio = px.line(
            df_filtrado,
            x='fecha_inicio_semana',
            y='precio_promedio_semanal',
            title=f'Precio Promedio Semanal por Kilo en {selected_lugar.title()}',
            labels={'fecha_inicio_semana': 'Semana', 'precio_promedio_semanal': 'Precio Promedio ($/Kg)'},
            markers=True, # Add markers to data points
            template='plotly_white' # Use a clean template
        )
        fig_precio.update_layout(
            xaxis_title="Semana del Año",
            yaxis_title="Precio Promedio ($/Kg)",
            title_x=0.5 # Center title
        )
        fig_precio.update_traces(line=dict(width=2)) # Make line slightly thicker

        # --- Create Head Count Figure ---
         # Create the figure for Total Head Count per Week
        fig_cantidad = px.bar(
            df_filtrado,
            x='fecha_inicio_semana',
            y='cantidad_total_semanal',
            title=f'Cantidad Total Semanal de Cabezas en {selected_lugar.title()}',
            labels={'fecha_inicio_semana': 'Semana', 'cantidad_total_semanal': 'Cantidad Total (Cabezas)'},
            template='plotly_white'
        )
        fig_cantidad.update_layout(
            xaxis_title="Semana del Año",
            yaxis_title="Cantidad Total (Cabezas)",
             title_x=0.5 # Center title
        )

        logging.info(f"Graphs generated successfully for {selected_lugar}")
        return fig_precio, fig_cantidad


    # Callback to export the price graph to HTML
    @app.callback(
        Output('export-feedback', 'children'), # Output to the feedback div
        Input('btn-export-precio', 'n_clicks'),
        State('dropdown-lugar', 'value'), # Get the currently selected location
        prevent_initial_call=True # Don't run when the app starts
    )
    def export_precio_graph(n_clicks, selected_lugar):
        if n_clicks > 0 and selected_lugar:
            logging.info(f"Export button clicked ({n_clicks} times) for location: {selected_lugar}")
            df_filtrado = df_final_agrupado[df_final_agrupado['lugar'] == selected_lugar].copy()
            df_filtrado = df_filtrado.sort_values('fecha_inicio_semana')

            if df_filtrado.empty:
                logging.warning(f"No data to export for location: {selected_lugar}")
                return f"No hay datos para exportar para {selected_lugar.title()}."


            # Regenerate the price figure (same logic as in update_graph)
            fig_precio_export = px.line(
                df_filtrado,
                x='fecha_inicio_semana',
                y='precio_promedio_semanal',
                title=f'Precio Promedio Semanal por Kilo en {selected_lugar.title()}',
                labels={'fecha_inicio_semana': 'Semana', 'precio_promedio_semanal': 'Precio Promedio ($/Kg)'},
                markers=True,
                template='plotly_white'
            )
            fig_precio_export.update_layout(
                xaxis_title="Semana del Año",
                yaxis_title="Precio Promedio ($/Kg)",
                title_x=0.5
            )
            fig_precio_export.update_traces(line=dict(width=2))

            # --- Export to HTML ---
            export_folder = "graficos_exportados"
            os.makedirs(export_folder, exist_ok=True) # Create export folder if needed
             # Sanitize location name for filename
            safe_lugar = "".join(c if c.isalnum() else "_" for c in selected_lugar)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"grafico_precio_{safe_lugar}_{timestamp}.html"
            filepath = os.path.join(export_folder, filename)

            try:
                fig_precio_export.write_html(filepath)
                logging.info(f"Price graph exported successfully to: {filepath}")
                # Provide user feedback
                return f"¡Gráfico exportado! Guardado como: {filepath}"
            except Exception as e:
                logging.error(f"Error exporting graph to HTML: {e}")
                return f"Error al exportar el gráfico: {e}"
        else:
             # Handle cases where button wasn't clicked or no location selected
             return "" # Return empty string initially or if no action needed


    # --- Run the Dash App ---
    logging.info("Launching Dash server...")
    app.run_server(debug=True, port=8050) # Use a specific port if needed, e.g., port=8050

    logging.info("Script finished.")
# --- End Main Execution and Dash App ---