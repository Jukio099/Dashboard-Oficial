# --- Código del Scraper (extract_auction_data, scrape_multiple_auctions, etc.) ---
# ... (Usa la versión completa anterior con Regex v7s) ...
import requests
import re
import pandas as pd
import io
import PyPDF2
import time
from datetime import datetime
import plotly.express as px
import os
import numpy as np

# --- Regex v7 (Simplificado) ---
row_pattern_v7_simplified = re.compile(
    r'^(\d+)'                          # 1: Lote
    r'([A-Z]{2})'                      # 2: Sexo
    r'(\d+?)'                          # 3: Can (non-greedy)
    r'([\d\.,]+?)'                     # 4: P. Total (non-greedy)
    r'\s*(\d+)'                        # 5: P. Prom (espacio opcional)
    r'\s*(.+?)\s+'                     # 6: Procedencia
    r'(\d{2}:\d{2}:\d{2})'             # 7: Entrada (Hora)
    r'\s+([\d\.,]+)'                   # 8: $Base
    r'\s+([\d\.,]+)'                   # 9: $Final
    r'\s*(.*)$'                        # 10: Observaciones
)
def clean_numeric(value_str):
    if isinstance(value_str, (int, float)): return int(value_str)
    if isinstance(value_str, str):
        cleaned = value_str.replace('.', '').replace(',', '').strip()
        if cleaned.isdigit(): return int(cleaned)
        return None
    return None
def extract_auction_data(auction_id, debug_mode=False):
    # ... (Código de extract_auction_data sin cambios respecto a la versión anterior) ...
    url = f"https://www.subacasanare.com/Precio_Pdf/{auction_id}"
    print_prefix = f"({auction_id}) " if not debug_mode else f"DEBUG ({auction_id}): "
    print(f"{print_prefix}Obteniendo PDF...", end='')
    try:
        response = requests.get(url, timeout=45); response.raise_for_status(); print(" OK", end='')
    except requests.exceptions.RequestException as e:
        print(f" Falló fetch: {type(e).__name__}"); return f"Error obteniendo PDF para subasta {auction_id}: {str(e)}"
    pdf_content_bytes = response.content; pdf_content = ""
    try:
        pdf_file = io.BytesIO(pdf_content_bytes); pdf_reader = PyPDF2.PdfReader(pdf_file); print(" Extrayendo texto...", end='')
        for i, page in enumerate(pdf_reader.pages):
            try:
                page_text = page.extract_text();
                if page_text: pdf_content += page_text + "\n"
            except Exception: pass
        print(" OK", end='')
        if not pdf_content: print(" Falló extracción texto!"); return f"Error: No texto PDF subasta {auction_id}."
    except Exception as e: print(f" Falló lectura PDF: {type(e).__name__}"); return f"Error leyendo PDF subasta {auction_id}: {str(e)}"
    header_info = {}; fecha_extraida = None
    try:
        feria_match = re.search(r'FERIA\s+NO\.\s+(\d+)', pdf_content); header_info['FERIA_NO'] = feria_match.group(1) if feria_match else None
        fecha_match = re.search(r'FECHA\s+FERIA\.\s+(\d{4}-\d{2}-\d{2})', pdf_content)
        if fecha_match: header_info['FECHA_FERIA'] = fecha_match.group(1); fecha_extraida = fecha_match.group(1)
    except Exception: pass
    lines = pdf_content.split('\n'); data = []; processed_lines = 0; unmatched_potential_lines = 0
    row_pattern = row_pattern_v7_simplified; print(" Parseando filas...", end='')
    potential_data_lines = []
    for line_num, line in enumerate(lines):
        line = line.strip()
        if line and re.match(r'^\d+', line) and re.search(r'\d{2}:\d{2}:\d{2}', line):
             if "Página" not in line and "LISTADO" not in line and "FECHA" not in line and "MARTILLO" not in line and "SUBASTA" not in line:
                 potential_data_lines.append((line_num + 1, line))
    for line_num, line in potential_data_lines:
        match = row_pattern.match(line)
        if match:
            try:
                groups = match.groups()
                if len(groups) == 10:
                    lote_str, sexo, cantidad_str, peso_total_str, peso_prom_str, procedencia_raw, hora_entrada, precio_base_str, precio_final_str, observaciones = [g.strip() if g else None for g in groups]
                    procedencia = re.sub(r'[?]', '', procedencia_raw).strip().upper() if procedencia_raw else None
                    lote = int(lote_str) if lote_str and lote_str.isdigit() else None; cantidad = int(cantidad_str) if cantidad_str and cantidad_str.isdigit() else None
                    peso_total = clean_numeric(peso_total_str); peso_promedio = clean_numeric(peso_prom_str)
                    precio_base = clean_numeric(precio_base_str); precio_final = clean_numeric(precio_final_str)

                    # --- VERIFICACIÓN OUTLIER (OPCIONAL DEBUG) ---
                    # if precio_final is not None and (precio_final < 1000 or precio_final > 20000):
                    #    print(f"\n  SOSPECHOSO ({auction_id}, Lote {lote}): Precio Final = {precio_final} (Raw: '{precio_final_str}') Linea: {line[:80]}...")

                    if all(v is not None for v in [lote, sexo, cantidad, peso_total, precio_final]) and fecha_extraida:
                        data.append([lote, sexo, cantidad, peso_total, peso_promedio, procedencia, hora_entrada, precio_base, precio_final, observaciones])
                        processed_lines += 1
                    # ... (resto del manejo de debug si falla el if all(...)) ...
                # ... (resto del manejo de debug si len(groups)!=10) ...
            except Exception as e: # ... (manejo de excepción con debug opcional) ...
                 if debug_mode: print(f"\n  DEBUG ({auction_id}): Error procesando fila {line_num}: {e} - línea: {line[:80]}...")
                 pass
        # ... (manejo de debug si regex falla) ...
        elif debug_mode: print(f"\n  DEBUG ({auction_id}): Regex v7s falló línea {line_num}: {line[:80]}...")

    if data: # ... (creación y retorno de DataFrame) ...
        columns = ['Lote', 'Sexo', 'Cantidad', 'Peso_Total', 'Peso_Promedio', 'Procedencia', 'Hora_Entrada', 'Precio_Base', 'Precio_Final', 'Observaciones']
        df = pd.DataFrame(data, columns=columns); # ... (añadir header/ID) ...
        if fecha_extraida: header_info['FECHA_FERIA'] = fecha_extraida
        for key, value in header_info.items():
             if value is not None: df[key] = value
        df['auction_id'] = auction_id; print(f" OK ({processed_lines} filas)")
        return df
    else: # ... (reporte de fallo) ...
        fail_reason = "datos tabla"; # ...
        print(f" Falló parseo: {fail_reason}"); return f"No se extrajeron {fail_reason} para subasta {auction_id}."

def scrape_multiple_auctions(start_id, end_id, delay=0.5, debug_mode=False):
    # ... (sin cambios) ...
    all_data = []; failed_auctions = []
    print(f"\nIniciando scraping desde la subasta {start_id} hasta {end_id}...")
    total_auctions = end_id - start_id + 1
    for i, auction_id in enumerate(range(start_id, end_id + 1)):
        result = extract_auction_data(auction_id, debug_mode=debug_mode)
        if isinstance(result, pd.DataFrame):
            if not result.empty: all_data.append(result)
            else: failed_auctions.append((auction_id, "Extracción devolvió DataFrame vacío"))
        else: failed_auctions.append((auction_id, result))
        if auction_id < end_id: time.sleep(delay)
    print("\n--- Scraping Finalizado ---")
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True); print(f"Total de filas extraídas: {len(combined_df)}"); print(f"Subastas procesadas con datos: {len(all_data)}")
        if failed_auctions: print(f"Subastas fallidas o vacías: {len(failed_auctions)}")
        return combined_df, failed_auctions
    else:
        print("❌ No se extrajeron datos de ninguna subasta en el rango.");
        if failed_auctions: print(f"Subastas fallidas o vacías: {len(failed_auctions)}")
        return None, failed_auctions


# === Ejecución Principal ===

# 1. Definir Rango
last_auction_id = 1824; num_auctions_to_scrape = 50
start_id = last_auction_id - num_auctions_to_scrape + 1; end_id = last_auction_id
print(f"Se extraerán las últimas {num_auctions_to_scrape} subastas (ID {start_id} a {end_id}).")

# 2. Ejecutar Scraping
print("--- Ejecutando Scraping (MODO NORMAL) ---")
combined_data, failures = scrape_multiple_auctions(start_id, end_id, delay=0.5, debug_mode=False)

# 3. Procesamiento Post-Scraping
if combined_data is not None and not combined_data.empty:
    # ... (Verificación rápida - opcional) ...

    print("\n--- Limpiando y Preparando ---")
    df_cleaned = combined_data.copy()
    if 'FECHA_FERIA' in df_cleaned.columns:
        df_cleaned['FECHA_FERIA'] = pd.to_datetime(df_cleaned['FECHA_FERIA'], errors='coerce')
    else: print("ADVERTENCIA: Columna 'FECHA_FERIA' no encontrada.")

    numeric_cols = ['Cantidad', 'Peso_Total', 'Peso_Promedio', 'Precio_Base', 'Precio_Final']
    for col in numeric_cols: df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')

    # --- *** FILTRO DE OUTLIERS EN PRECIO FINAL *** ---
    precio_min_plausible = 1000
    precio_max_plausible = 20000 # Ajusta este valor si es necesario
    initial_rows_before_outlier_filter = len(df_cleaned)
    df_cleaned = df_cleaned[
        (df_cleaned['Precio_Final'] >= precio_min_plausible) &
        (df_cleaned['Precio_Final'] <= precio_max_plausible) &
        (df_cleaned['Precio_Final'].notna()) # Asegurarse de no filtrar NaNs aquí
    ]
    rows_after_outlier_filter = len(df_cleaned)
    print(f"Filtrado de Outliers (Precio Final [{precio_min_plausible}-{precio_max_plausible}]): {initial_rows_before_outlier_filter - rows_after_outlier_filter} filas eliminadas.")
    # --- *** FIN FILTRO OUTLIERS *** ---

    # Definir columnas esenciales y determinar columna de agrupación
    essential_cols = ['Sexo', 'Cantidad', 'Peso_Total', 'Precio_Final']
    grouping_col = 'auction_id'
    if 'FECHA_FERIA' in df_cleaned.columns and pd.api.types.is_datetime64_any_dtype(df_cleaned['FECHA_FERIA']) and not df_cleaned['FECHA_FERIA'].isnull().all():
        essential_cols.append('FECHA_FERIA')
        grouping_col = 'FECHA_FERIA'
        aggregation_type = "SEMANAL"
        print("Agrupación será SEMANAL por 'FECHA_FERIA'.")
    else:
        aggregation_type = "POR_SUBASTA"
        print(f"ADVERTENCIA: Agrupación será POR SUBASTA ('{grouping_col}') debido a problemas con 'FECHA_FERIA'.")

    # Quitar NAs en esenciales
    initial_rows = len(df_cleaned); df_cleaned.dropna(subset=essential_cols, inplace=True); rows_after_na = len(df_cleaned)
    print(f"Filas después de quitar NAs esenciales: {rows_after_na} ({initial_rows - rows_after_na} eliminadas)")

    if rows_after_na > 0:
        print(f"\n--- Calculando Promedios ({aggregation_type}) ---")
        df_cleaned = df_cleaned[(df_cleaned['Cantidad'] > 0) & (df_cleaned['Peso_Total'] > 0)]
        df_cleaned['Valor_Total_Lote'] = df_cleaned['Precio_Final'] * df_cleaned['Peso_Total']

        if aggregation_type == 'SEMANAL':
            df_agrupado = df_cleaned.groupby([pd.Grouper(key='FECHA_FERIA', freq='W-MON'), 'Sexo']).agg(
                Valor_Total_Categoria=('Valor_Total_Lote', 'sum'), Peso_Total_Categoria=('Peso_Total', 'sum'), Cantidad_Lotes=('Lote', 'count')
            ).reset_index()
        else: # POR_SUBASTA
            df_agrupado = df_cleaned.groupby([grouping_col, 'Sexo']).agg(
                Valor_Total_Categoria=('Valor_Total_Lote', 'sum'), Peso_Total_Categoria=('Peso_Total', 'sum'), Cantidad_Lotes=('Lote', 'count')
            ).reset_index()

        df_agrupado['Precio_Promedio_Ponderado'] = df_agrupado['Valor_Total_Categoria'] / df_agrupado['Peso_Total_Categoria']
        df_agrupado.replace([np.inf, -np.inf], np.nan, inplace=True); df_agrupado.dropna(subset=['Precio_Promedio_Ponderado'], inplace=True)
        print(f"Datos agrupados ({aggregation_type}) listos ({len(df_agrupado)} filas).")

        # --- Guardar Resultados ---
        output_folder = "resultados_subastas"; os.makedirs(output_folder, exist_ok=True)
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Guardar datos limpios (antes de agrupar) y agrupados
            clean_data_filename = os.path.join(output_folder, f"datos_limpios_{start_id}_a_{end_id}_{timestamp}.csv")
            grouped_filename = os.path.join(output_folder, f"datos_agrupados_{aggregation_type}_{start_id}_a_{end_id}_{timestamp}.csv")
            df_cleaned.to_csv(clean_data_filename, index=False, encoding='utf-8-sig')
            df_agrupado.to_csv(grouped_filename, index=False, encoding='utf-8-sig')
            print(f"Datos guardados en carpeta: '{output_folder}'")
            # Guardaremos las gráficas con Dash ahora
        except Exception as e: print(f"❌ Error guardando archivos CSV: {e}")

        # --- MENSAJE: Pasar a Dash para Gráficas Avanzadas ---
        print("\n--- Siguientes Pasos: Visualización Interactiva con Dash ---")
        print("Los datos limpios y agrupados se han guardado en CSV.")
        print("Para crear gráficas con filtros desplegables, sliders, etc.,")
        print("se recomienda usar la librería Dash.")
        print("\nA continuación se muestra un EJEMPLO BÁSICO de cómo usar Dash con los datos guardados.")
        print("Este ejemplo debe guardarse y ejecutarse como un SCRIPT SEPARADO.")
        print("Necesitarás instalar Dash: pip install dash")

    else: print("No quedaron suficientes datos después de la limpieza para continuar.")
# ... (Manejo si combined_data era None o falló) ...
else:
    print("\nNo se pudieron obtener datos iniciales o falló el scraping.")

# === FIN DEL SCRIPT DE SCRAPING Y PROCESAMIENTO ===