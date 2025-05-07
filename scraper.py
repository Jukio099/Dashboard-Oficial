# scraper_tabula.py (Versión con Tabula-py)

import requests
import re
import pandas as pd
import io
import tabula # <--- Nueva librería para extraer tablas
import PyPDF2 # <-- Aún se usa para extraer metadatos (Fecha, Feria No)
import time
from datetime import datetime
import os
import numpy as np
import logging

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Constantes ---
BASE_URL = "https://www.subacasanare.com/Precio_Pdf/"
OUTPUT_FOLDER = "resultados_subastas"
FILENAME_PREFIX_LIMPIOS = "datos_limpios"
FILENAME_PREFIX_AGRUPADOS_SEMANAL = "datos_agrupados_SEMANAL"
REQUEST_TIMEOUT = 45
INTER_REQUEST_DELAY = 0.8 
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

EXPECTED_COLUMNS = {
    'Lote': 'Lote',
    'Sexo': 'Sexo',
    'Can': 'Cantidad',
    'P. Total': 'Peso_Total', 
    'P.Prom': 'Peso_Promedio',
    'Procedencia': 'Procedencia',
    'Entrada': 'Hora_Entrada',
    '$Base': 'Precio_Base',
    '$Final': 'Precio_Final',
    'Observaciones': 'Observaciones'
}

# --- Funciones Auxiliares ---
def clean_numeric(value_str, field_name="valor", auction_id=None, lote=None):
    if pd.isna(value_str): return None
    if isinstance(value_str, (int, float)): return int(value_str)
    if isinstance(value_str, str):
        cleaned = str(value_str).strip().replace('.', '').replace(',', '')
        if cleaned.isdigit(): return int(cleaned)
        else:
            cleaned_alt = re.sub(r'[^\d]', '', str(value_str))
            if cleaned_alt.isdigit() and cleaned_alt:
                 logging.debug(f"Scraper ({auction_id}, Lote {lote}): Limpieza alternativa para entero '{field_name}': '{value_str}' -> '{cleaned_alt}'")
                 return int(cleaned_alt)
            logging.warning(f"Scraper ({auction_id}, Lote {lote}): No se pudo convertir '{field_name}' a número: '{value_str}' (Limpiado: '{cleaned}')")
            return None
    logging.warning(f"Scraper ({auction_id}, Lote {lote}): Tipo inesperado para '{field_name}': {type(value_str)} ('{value_str}')")
    return None

def clean_currency(value_str, field_name="precio", auction_id=None, lote=None):
    if pd.isna(value_str): return None
    if isinstance(value_str, (int, float)): 
        # Si ya es numérico, verificar si necesita multiplicarse por 1000
        if value_str < 100:  # Si el valor es menor a 100, probablemente está en miles
            return float(value_str) * 1000
        return float(value_str)
        
    if isinstance(value_str, str):
        cleaned_val = str(value_str).strip().replace('$', '')
        # Primero intentamos limpiar el valor manteniendo los puntos y comas
        if ',' in cleaned_val and '.' in cleaned_val: 
            if cleaned_val.rfind(',') > cleaned_val.rfind('.'): 
                cleaned_val = cleaned_val.replace('.', '').replace(',', '.')
            else: 
                cleaned_val = cleaned_val.replace(',', '')
        elif ',' in cleaned_val: 
             cleaned_val = cleaned_val.replace('.', '') 
             if cleaned_val.count(',') > 1: 
                  parts = cleaned_val.split(',')
                  cleaned_val = "".join(parts[:-1]) + "." + parts[-1]
             else: 
                  cleaned_val = cleaned_val.replace(',', '.')
        elif '.' in cleaned_val and cleaned_val.count('.') > 1: 
            parts = cleaned_val.split('.')
            cleaned_val = "".join(parts[:-1]) + "." + parts[-1]
            
        cleaned_val = re.sub(r'[^\d.]', '', cleaned_val) 
        try:
            if cleaned_val.count('.') > 1:
                first_dot = cleaned_val.find('.')
                cleaned_val = cleaned_val[:first_dot+1] + cleaned_val[first_dot+1:].replace('.', '')
            if not cleaned_val: return None 
            
            # Convertir a float y multiplicar por 1000 si el valor es menor a 100
            float_val = float(cleaned_val)
            if float_val < 100:  # Si el valor es menor a 100, probablemente está en miles
                float_val *= 1000
                
            return float_val
        except ValueError:
             logging.warning(f"Scraper ({auction_id}, Lote {lote}): No se pudo convertir '{field_name}' a float: '{value_str}' (Limpiado: '{cleaned_val}')")
             return None
    logging.warning(f"Scraper ({auction_id}, Lote {lote}): Tipo inesperado para '{field_name}': {type(value_str)} ('{value_str}')")
    return None

# --- Funciones Principales ---
def extract_metadata_from_pdf(pdf_content_bytes, auction_id):
    header_info = {'FERIA_NO': str(auction_id), 'FECHA_FERIA': None}
    if not pdf_content_bytes:
        logging.error(f"Scraper ({auction_id}): No hay contenido de bytes para extraer metadatos.")
        return header_info
    try:
        pdf_file = io.BytesIO(pdf_content_bytes)
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        if pdf_reader.pages:
            first_page_text = pdf_reader.pages[0].extract_text()
            if first_page_text:
                feria_match = re.search(r'FERIA\s+NO\.?\s*(\d+)', first_page_text, re.IGNORECASE)
                if feria_match: header_info['FERIA_NO'] = feria_match.group(1).strip()
                
                fecha_match = re.search(r'FECHA\s+FERIA\.?\s*(\d{4}-\d{2}-\d{2})', first_page_text, re.IGNORECASE)
                if fecha_match:
                    fecha_extraida_str = fecha_match.group(1).strip()
                    try:
                        fecha_dt = pd.to_datetime(fecha_extraida_str)
                        header_info['FECHA_FERIA'] = fecha_dt.strftime('%Y-%m-%d')
                        logging.info(f"Scraper ({auction_id}): Metadatos extraídos - Fecha: {header_info['FECHA_FERIA']}, Feria No: {header_info['FERIA_NO']}")
                    except ValueError:
                        logging.error(f"Scraper ({auction_id}): Fecha extraída '{fecha_extraida_str}' no es válida.")
                else: logging.warning(f"Scraper ({auction_id}): No se encontró Fecha Feria en la primera página.")
            else: logging.warning(f"Scraper ({auction_id}): No se pudo extraer texto de la primera página para metadatos.")
        else: logging.warning(f"Scraper ({auction_id}): El PDF no tiene páginas para extraer metadatos.")
    except Exception as e:
        logging.error(f"Scraper ({auction_id}): Falló la extracción de metadatos del PDF: {e}", exc_info=True)
    return header_info

def parse_pdf_with_tabula(pdf_content_bytes, auction_id):
    if not pdf_content_bytes:
        logging.error(f"Scraper ({auction_id}): No hay contenido de bytes para parsear con Tabula.")
        return None

    # 1. 'TABLE_AREA_COORDINATES': [top, left, bottom, right]
    #    Asegúrate que 'top' esté JUSTO ENCIMA de la fila de encabezados "Lote", "Sexo", etc.
    TABLE_AREA_COORDINATES = [130, 25, 750, 585] # <--- VERIFICA ESTE 'top' (130) Y DEMÁS.

    # 2. 'COLUMN_X_COORDINATES': ¡¡¡ESTA ES LA PARTE MÁS IMPORTANTE QUE DEBES EDITAR!!!
    #    Mide en tu PDF (en PUNTOS) la posición X del BORDE DERECHO de cada columna.
    #    Reemplaza los valores de ejemplo de abajo con TUS MEDICIONES PRECISAS.
    #    Si tienes 10 columnas visualmente, normalmente necesitas 9 valores X aquí (el borde derecho de las primeras 9).
    COLUMN_X_COORDINATES = [
        # Formato: [X_fin_Lote, X_fin_Sexo, X_fin_Can, X_fin_PTotal, X_fin_PProm, X_fin_Proced, X_fin_Entra, X_fin_Base, X_fin_Final]
        # ¡¡¡REEMPLAZA ESTOS VALORES DE EJEMPLO CON TUS MEDICIONES!!!
        0, # X_FIN_LOTE        (Ej: Si 'Lote' termina en X=60, pon 60)
        0, # X_FIN_SEXO        (Ej: Si 'Sexo' termina en X=100, pon 100)
        0, # X_FIN_CAN         (Ej: Si 'Can' termina en X=130, pon 130)
        0, # X_FIN_PTOTAL      (Ej: Si 'P. Total' termina en X=200, pon 200)
        0, # X_FIN_PPROM       (Ej: Si 'P.Prom' termina en X=250, pon 250)
        0, # X_FIN_PROCEDENCIA (Ej: Si 'Procedencia' termina en X=340, pon 340)
        0, # X_FIN_ENTRADA     (Ej: Si 'Entrada' termina en X=400, pon 400)
        0, # X_FIN_BASE        (Ej: Si '$Base' termina en X=460, pon 460)
        0  # X_FIN_FINAL       (Ej: Si '$Final' termina en X=525, pon 525)
        # No necesitas la X para 'Observaciones' si es la última y quieres que tome hasta el final del 'area.right'.
        # Si la lista está vacía o con ceros, el script te advertirá.
    ]
    # Verifica que no hayas dejado los ceros de ejemplo; ¡deben ser tus mediciones!
    if all(v == 0 for v in COLUMN_X_COORDINATES) or not COLUMN_X_COORDINATES:
        logging.critical(f"Scraper ({auction_id}): ¡ALERTA! 'COLUMN_X_COORDINATES' contiene solo ceros o está vacía. "
                         "DEBES REEMPLAZAR ESTOS VALORES CON TUS MEDICIONES DEL PDF PARA QUE LAS COLUMNAS SE DIVIDAN CORRECTAMENTE. "
                         "El script intentará continuar sin 'columns', lo que probablemente resulte en columnas mal divididas.")
        active_column_x_coords = None # Forzar a no usar 'columns' si no se han editado
    else:
        active_column_x_coords = COLUMN_X_COORDINATES


    logging.info(f"Scraper ({auction_id}): Usando coordenadas de área: {TABLE_AREA_COORDINATES}")
    if active_column_x_coords:
        logging.info(f"Scraper ({auction_id}): Usando coordenadas X de columnas para stream: {active_column_x_coords}")

    all_tables = []
    extraction_method_used = "ninguno"
    pdf_file_like = io.BytesIO(pdf_content_bytes)
    pandas_opts = {'header': 0} 

    # Intento 1: stream=True, con area y CON 'columns' (si se definieron)
    if active_column_x_coords: 
        try:
            logging.info(f"Scraper ({auction_id}): Intento 1: stream=True, area={TABLE_AREA_COORDINATES}, columns={active_column_x_coords}...")
            current_tables = tabula.read_pdf(pdf_file_like, pages='all', multiple_tables=True, stream=True, guess=False, 
                                                area=TABLE_AREA_COORDINATES, columns=active_column_x_coords, pandas_options=pandas_opts)
            if current_tables and not all(df.empty for df in current_tables if df is not None):
                all_tables = [df for df in current_tables if df is not None and not df.empty]
                if all_tables:
                    extraction_method_used = f"stream, area, columns"
                    logging.info(f"Scraper ({auction_id}): Tabula ({extraction_method_used}) encontró {len(all_tables)} tabla(s) no vacía(s).")
            else:
                logging.info(f"Scraper ({auction_id}): Intento 1 (stream, area, columns) no encontró tablas o todas estaban vacías.")
        except Exception as e:
            logging.warning(f"Scraper ({auction_id}): Intento 1 (stream, area, columns) falló con error: {e}", exc_info=False)
        pdf_file_like.seek(0)

    # Intento 2: stream=True, con area PERO SIN 'columns'
    if not all_tables:
        try:
            logging.info(f"Scraper ({auction_id}): Intento 2: stream=True, area={TABLE_AREA_COORDINATES} (SIN columns)...")
            current_tables = tabula.read_pdf(pdf_file_like, pages='all', multiple_tables=True, stream=True, guess=True, 
                                             area=TABLE_AREA_COORDINATES, pandas_options=pandas_opts)
            if current_tables and not all(df.empty for df in current_tables if df is not None):
                all_tables = [df for df in current_tables if df is not None and not df.empty]
                if all_tables:
                    extraction_method_used = f"stream, area (SIN columns)"
                    logging.info(f"Scraper ({auction_id}): Tabula ({extraction_method_used}) encontró {len(all_tables)} tabla(s) no vacía(s).")
            else:
                logging.info(f"Scraper ({auction_id}): Intento 2 (stream, area SIN columns) no encontró tablas o todas estaban vacías.")
        except Exception as e:
            logging.warning(f"Scraper ({auction_id}): Intento 2 (stream, area SIN columns) falló con error: {e}", exc_info=False)
        pdf_file_like.seek(0)
    
    # Intento 3: lattice=True, con area
    if not all_tables:
        try:
            logging.info(f"Scraper ({auction_id}): Intento 3: lattice=True, area={TABLE_AREA_COORDINATES}...")
            current_tables = tabula.read_pdf(pdf_file_like, pages='all', multiple_tables=True, lattice=True, guess=False, area=TABLE_AREA_COORDINATES, pandas_options=pandas_opts)
            if current_tables and not all(df.empty for df in current_tables if df is not None):
                all_tables = [df for df in current_tables if df is not None and not df.empty] 
                if all_tables:
                    extraction_method_used = f"lattice, area={TABLE_AREA_COORDINATES}"
                    logging.info(f"Scraper ({auction_id}): Tabula ({extraction_method_used}) encontró {len(all_tables)} tabla(s) no vacía(s).")
            else:
                logging.info(f"Scraper ({auction_id}): Intento 3 (lattice, area) no encontró tablas o todas estaban vacías.")
        except Exception as e:
            logging.warning(f"Scraper ({auction_id}): Intento 3 (lattice, area) falló con error: {e}", exc_info=False)
        pdf_file_like.seek(0)

    # Intento 4: stream=True, SIN area (fallback más general)
    if not all_tables:
        try:
            logging.info(f"Scraper ({auction_id}): Intento 4: stream=True, SIN area (puede capturar encabezados/pies)...")
            current_tables = tabula.read_pdf(pdf_file_like, pages='all', multiple_tables=True, stream=True, guess=True, pandas_options=pandas_opts)
            if current_tables and not all(df.empty for df in current_tables if df is not None):
                all_tables = [df for df in current_tables if df is not None and not df.empty]
                if all_tables:
                    extraction_method_used = "stream, SIN area"
                    logging.info(f"Scraper ({auction_id}): Tabula ({extraction_method_used}) encontró {len(all_tables)} tabla(s) no vacía(s).")
            else:
                logging.info(f"Scraper ({auction_id}): Intento 4 (stream, SIN area) no encontró tablas o todas estaban vacías.")
        except Exception as e:
            logging.warning(f"Scraper ({auction_id}): Intento 4 (stream, SIN area) falló con error: {e}", exc_info=True) 

    if not all_tables:
        logging.warning(f"Scraper ({auction_id}): DESPUÉS DE TODOS LOS INTENTOS, Tabula no extrajo ninguna tabla válida del PDF.")
        return pd.DataFrame()
    
    logging.info(f"Scraper ({auction_id}): Se procesarán {len(all_tables)} tabla(s) extraídas con el método: {extraction_method_used}.")
    
    valid_data_frames = []
    min_expected_cols = 9 
    min_rows_for_valid_table = 3 

    for i, df_raw in enumerate(all_tables): 
        logging.debug(f"Scraper ({auction_id}): Procesando tabla {i+1}/{len(all_tables)} (de {extraction_method_used}) con {df_raw.shape[1]} columnas y {df_raw.shape[0]} filas.")
        df = df_raw.copy()

        logging.info(f"Scraper ({auction_id}): --- INICIO DEBUG TABLA {i+1} (Método: {extraction_method_used}) ---")
        logging.info(f"Scraper ({auction_id}): Tabla {i+1} - Forma (filas, columnas): {df.shape}")
        original_columns = [str(col).replace('\r', ' ').replace('\n', ' ') for col in df.columns]
        logging.info(f"Scraper ({auction_id}): Tabla {i+1} - Nombres de columnas originales: {original_columns}")
        try:
            head_str = df.head(3).to_string() 
            logging.info(f"Scraper ({auction_id}): Tabla {i+1} - Primeras 3 filas (datos brutos):\n{head_str}")
        except Exception as e_log_head:
            logging.error(f"Scraper ({auction_id}): Error al convertir cabecera de tabla a string para log: {e_log_head}")
        logging.info(f"Scraper ({auction_id}): --- FIN DEBUG TABLA {i+1} ---")

        df.columns = [' '.join(str(col).split()).strip() for col in df.columns]

        if df.shape[0] >= min_rows_for_valid_table and df.shape[1] >= min_expected_cols:
            has_lote = any(col.lower().strip() == 'lote' for col in df.columns)
            has_sexo = any(col.lower().strip() == 'sexo' for col in df.columns) 
            has_can = any(col.lower().strip() == 'can' for col in df.columns) 
            has_peso_total = any(re.fullmatch(r'p\s*\.\s*total|peso\s*total', col.lower().strip()) for col in df.columns)
            has_precio_final = any((re.fullmatch(r'\$final|final', col.lower().strip()) and 'kg' not in col.lower()) for col in df.columns)
            
            present_expected_cols_count = 0
            temp_renamed_cols_check = {}
            for expected_key_search, target_name_search in EXPECTED_COLUMNS.items():
                normalized_expected_search = expected_key_search.lower().replace('.', '').replace(' ', '').replace('$', '')
                for raw_col_search in df.columns: 
                    normalized_raw_search = str(raw_col_search).lower().replace('.', '').replace(' ', '').replace('$', '')
                    if normalized_raw_search == normalized_expected_search: 
                        if raw_col_search not in temp_renamed_cols_check: 
                             temp_renamed_cols_check[raw_col_search] = target_name_search
                             present_expected_cols_count+=1
                             break 
            
            if has_lote and has_sexo and has_can and (has_peso_total or has_precio_final) and present_expected_cols_count >= 6 : 
                logging.info(f"Scraper ({auction_id}): Tabla {i+1} PARECE ser una tabla de lotes VÁLIDA (Columnas: {list(df.columns)}). Criterios: Lote={has_lote}, Sexo={has_sexo}, Can={has_can}, P.Total={has_peso_total}, $Final={has_precio_final}, Coincidencias exactas={present_expected_cols_count}")
                valid_data_frames.append(df)
            else:
                 logging.debug(f"Scraper ({auction_id}): Tabla {i+1} DESCARTADA (no cumple criterios estrictos). Criterios: Lote={has_lote}, Sexo={has_sexo}, Can={has_can}, P.Total={has_peso_total}, $Final={has_precio_final}, Coincidencias exactas={present_expected_cols_count}. Columnas: {list(df.columns)}")
        elif df.shape[0] < min_rows_for_valid_table:
            logging.debug(f"Scraper ({auction_id}): Tabla {i+1} descartada (tiene {df.shape[0]} filas, se esperaban >={min_rows_for_valid_table}).")
        else: 
            logging.debug(f"Scraper ({auction_id}): Tabla {i+1} descartada (tiene {df.shape[1]} cols ({list(df.columns)}), se esperaban >={min_expected_cols}).")

    if not valid_data_frames:
        logging.warning(f"Scraper ({auction_id}): No se identificó NINGUNA tabla de lotes válida después de la validación (método: {extraction_method_used}).")
        return pd.DataFrame()

    try:
        combined_df = pd.concat(valid_data_frames, ignore_index=True)
        logging.info(f"Scraper ({auction_id}): {len(valid_data_frames)} tablas de lotes concatenadas. Total filas brutas: {len(combined_df)}")
    except Exception as e_concat:
         logging.error(f"Scraper ({auction_id}): Error concatenando tablas extraídas: {e_concat}", exc_info=True)
         return None

    potential_col_map = {}
    combined_df.columns = [str(col) for col in combined_df.columns] 
    
    for expected_key, target_name in EXPECTED_COLUMNS.items():
        best_match_col = None
        normalized_expected = expected_key.lower().replace('.', '').replace(' ', '').replace('$', '')
        
        for raw_col in combined_df.columns:
            if raw_col in potential_col_map: continue 
            normalized_raw = raw_col.lower().replace('.', '').replace(' ', '').replace('$', '')
            if normalized_raw == normalized_expected:
                best_match_col = raw_col; break
        if best_match_col: potential_col_map[best_match_col] = target_name; logging.debug(f"Mapeo exacto para '{expected_key}' -> '{target_name}' usó '{best_match_col}'."); continue

        if len(normalized_expected) >= 3:
            for raw_col in combined_df.columns:
                if raw_col in potential_col_map: continue
                normalized_raw = raw_col.lower().replace('.', '').replace(' ', '').replace('$', '')
                if normalized_raw.startswith(normalized_expected):
                    best_match_col = raw_col; break
            if best_match_col: potential_col_map[best_match_col] = target_name; logging.debug(f"Mapeo 'startswith' para '{expected_key}' -> '{target_name}' usó '{best_match_col}'."); continue
        
        if expected_key.startswith('$'):
            for raw_col in combined_df.columns:
                if raw_col in potential_col_map: continue
                if raw_col.startswith(expected_key): 
                    best_match_col = raw_col; break
            if best_match_col: potential_col_map[best_match_col] = target_name; logging.debug(f"Mapeo '$' para '{expected_key}' -> '{target_name}' usó '{best_match_col}'."); continue
        
        if len(normalized_expected) >= 3: 
            for raw_col in combined_df.columns:
                if raw_col in potential_col_map: continue
                normalized_raw = raw_col.lower().replace('.', '').replace(' ', '').replace('$', '')
                if normalized_expected in normalized_raw:
                    best_match_col = raw_col; logging.debug(f"Mapeo (contiene) para '{expected_key}' -> '{target_name}' usó '{raw_col}'."); break 
        if best_match_col: potential_col_map[best_match_col] = target_name; continue
            
        logging.warning(f"Scraper ({auction_id}): No se encontró columna para mapear a '{expected_key}'.")

    logging.info(f"Scraper ({auction_id}): Mapa de columnas propuesto: {potential_col_map}")
    combined_df.rename(columns=potential_col_map, inplace=True)
    
    final_columns_present = [col for col in EXPECTED_COLUMNS.values() if col in combined_df.columns]
    final_df = combined_df[final_columns_present].copy()

    if 'Lote' not in final_df.columns:
        logging.error(f"Scraper ({auction_id}): Columna 'Lote' ESENCIAL no encontrada después del renombrado final.")
        return pd.DataFrame() 
        
    final_df['Lote'] = final_df['Lote'].apply(lambda x: clean_numeric(x, "Lote", auction_id))
    final_df.dropna(subset=['Lote'], inplace=True) 
    if final_df.empty: 
        logging.warning(f"Scraper ({auction_id}): DataFrame vacío después de dropear Lotes NaN.")
        return pd.DataFrame()
    try:
        final_df['Lote'] = final_df['Lote'].astype(int)
    except ValueError as e_lote_int:
        logging.error(f"Scraper ({auction_id}): No se pudo convertir 'Lote' a entero: {e_lote_int}")
        logging.error(f"Valores problemáticos en Lote: {final_df[pd.to_numeric(final_df['Lote'], errors='coerce').isna()]['Lote'].unique()[:5]}")
        return pd.DataFrame()

    for col_name, clean_func in {
        'Cantidad': clean_numeric, 'Peso_Total': clean_numeric,
        'Peso_Promedio': clean_numeric, 'Precio_Base': clean_currency,
        'Precio_Final': clean_currency
    }.items():
        if col_name in final_df.columns:
            final_df[col_name] = final_df.apply(lambda row: clean_func(row[col_name], col_name, auction_id, row.get('Lote')), axis=1)
            final_df[col_name] = pd.to_numeric(final_df[col_name], errors='coerce')

    for col_cat in ['Sexo', 'Procedencia', 'Observaciones', 'Hora_Entrada']:
        if col_cat in final_df.columns:
            final_df[col_cat] = final_df[col_cat].astype(str).str.upper().str.strip().replace('NAN', pd.NA).replace('NONE', pd.NA)

    essential_cols_for_analysis = ['Lote', 'Sexo', 'Cantidad', 'Peso_Total', 'Precio_Final']
    cols_to_check_na = [col for col in essential_cols_for_analysis if col in final_df.columns]
    if cols_to_check_na: final_df.dropna(subset=cols_to_check_na, how='any', inplace=True)
    
    for col_num_positive in ['Cantidad', 'Peso_Total', 'Precio_Final']:
        if col_num_positive in final_df.columns:
            final_df[col_num_positive] = pd.to_numeric(final_df[col_num_positive], errors='coerce')
            final_df.dropna(subset=[col_num_positive], inplace=True)
            if not final_df.empty: final_df = final_df[final_df[col_num_positive] > 0]
            
    logging.info(f"Scraper ({auction_id}): Filas después de limpieza y filtrado final: {len(final_df)}")
    if final_df.empty:
         logging.warning(f"Scraper ({auction_id}): No quedaron filas válidas después de la limpieza y filtrado final.")
    return final_df

def extract_auction_data_tabula(auction_id):
    url = f"{BASE_URL}{auction_id}"
    logging.info(f"Scraper ({auction_id}): Intentando obtener PDF desde {url}...")
    pdf_content_bytes = None
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT, headers=REQUEST_HEADERS)
        logging.info(f"Scraper ({auction_id}): Petición GET enviada. Status Code: {response.status_code}")
        response.raise_for_status() 
        pdf_content_bytes = response.content
        logging.info(f"Scraper ({auction_id}): PDF descargado ({len(pdf_content_bytes)} bytes).")
    except requests.exceptions.HTTPError as e_http:
         if response.status_code == 404: 
              logging.warning(f"Scraper ({auction_id}): PDF no encontrado (404) en {url}.")
         else:
              logging.error(f"Scraper ({auction_id}): Error HTTP {response.status_code} descargando PDF de {url}: {e_http}")
         return None 
    except requests.exceptions.Timeout:
        logging.error(f"Scraper ({auction_id}): Timeout ({REQUEST_TIMEOUT}s) descargando PDF de {url}.")
        return None
    except requests.exceptions.RequestException as e_req:
        logging.error(f"Scraper ({auction_id}): Error de red descargando PDF de {url}: {e_req}")
        return None
    except Exception as e_general_download: 
         logging.error(f"Scraper ({auction_id}): Error inesperado durante la descarga de {url}: {e_general_download}", exc_info=True)
         return None

    metadata = extract_metadata_from_pdf(pdf_content_bytes, auction_id)
    if metadata.get('FECHA_FERIA') is None: 
         logging.warning(f"Scraper ({auction_id}): No se pudo extraer la fecha de los metadatos.")

    df_auction = parse_pdf_with_tabula(pdf_content_bytes, auction_id)

    base_meta_df = pd.DataFrame({
        'auction_id': [auction_id],
        'FERIA_NO': [metadata.get('FERIA_NO', str(auction_id))],
        'FECHA_FERIA': [pd.to_datetime(metadata.get('FECHA_FERIA'), errors='coerce')]
    })

    if df_auction is None: 
        logging.error(f"Scraper ({auction_id}): Fallo crítico en parse_pdf_with_tabula, devolvió None.")
        return base_meta_df
    elif df_auction.empty: 
        logging.warning(f"Scraper ({auction_id}): parse_pdf_with_tabula devolvió un DataFrame vacío.")
        return base_meta_df 
    else:
        for col, val in base_meta_df.iloc[0].items():
            if col not in df_auction.columns or df_auction[col].isnull().all():
                 df_auction[col] = val
            elif col == 'FECHA_FERIA' and not pd.isna(val): 
                 if df_auction[col].iloc[0] != val:
                      logging.warning(f"Scraper ({auction_id}): Discrepancia de fechas. Metadatos: {val}, Tabla: {df_auction[col].iloc[0]}. Usando fecha de metadatos.")
                      df_auction[col] = val
        return df_auction

def scrape_multiple_auctions_tabula(start_id, end_id):
    all_data_frames_with_content = [] 
    processed_auction_metadata = [] 
    
    logging.info(f"--- Iniciando scraping (Tabula) desde subasta {start_id} hasta {end_id} ---")
    total_auctions_to_process = end_id - start_id + 1

    for i, auction_id in enumerate(range(start_id, end_id + 1)):
        logging.info(f"Procesando subasta {i+1}/{total_auctions_to_process} (ID: {auction_id})...")
        result_df = extract_auction_data_tabula(auction_id)

        feria_no_val = str(auction_id)
        fecha_feria_val = pd.NaT
        status_val = 'unknown_error_or_df_none' 
        rows_extracted = 0

        if result_df is not None:
            feria_no_val = result_df['FERIA_NO'].iloc[0] if 'FERIA_NO' in result_df.columns and not result_df.empty and not pd.isna(result_df['FERIA_NO'].iloc[0]) else str(auction_id)
            fecha_feria_val = result_df['FECHA_FERIA'].iloc[0] if 'FECHA_FERIA' in result_df.columns and not result_df.empty and not pd.isna(result_df['FECHA_FERIA'].iloc[0]) else pd.NaT
            
            essential_data_cols = ['Lote', 'Sexo', 'Cantidad', 'Peso_Total', 'Precio_Final']
            has_valid_lot_data = not result_df.empty and \
                                 all(col in result_df.columns for col in essential_data_cols) and \
                                 not result_df[essential_data_cols].isnull().all().all()

            if has_valid_lot_data:
                all_data_frames_with_content.append(result_df)
                rows_extracted = len(result_df)
                logging.info(f"Scraper ({auction_id}): Éxito, {rows_extracted} filas de datos de lotes añadidas.")
                status_val = 'data_extracted'
            else: 
                logging.warning(f"Scraper ({auction_id}): Procesado, pero no se encontraron datos de lotes válidos.")
                status_val = 'no_lot_data_found'
                if result_df.empty or ('Lote' not in result_df.columns and 'FERIA_NO' not in result_df.columns): 
                     status_val = 'parse_returned_empty_or_no_real_metadata'
        else: 
            logging.error(f"Scraper ({auction_id}): Fallo crítico, extract_auction_data_tabula devolvió None.")
            
        processed_auction_metadata.append({
            'auction_id': auction_id, 
            'FERIA_NO': feria_no_val, 
            'FECHA_FERIA': fecha_feria_val, 
            'status': status_val, 
            'rows': rows_extracted
        })

        if auction_id < end_id: time.sleep(INTER_REQUEST_DELAY)

    logging.info("--- Scraping (Tabula) Finalizado ---")
    
    summary_df = pd.DataFrame(processed_auction_metadata)
    auctions_with_issues = []
    if not summary_df.empty:
        os.makedirs(OUTPUT_FOLDER, exist_ok=True) 
        summary_filename = os.path.join(OUTPUT_FOLDER, f"resumen_procesamiento_{start_id}_a_{end_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        try:
            summary_df.to_csv(summary_filename, index=False, date_format='%Y-%m-%d')
            logging.info(f"Resumen del procesamiento guardado en: {summary_filename}")
        except Exception as e_save_summary:
            logging.error(f"No se pudo guardar el resumen del procesamiento: {e_save_summary}")
        auctions_with_issues = summary_df[summary_df['status'] != 'data_extracted']['auction_id'].tolist()

    if all_data_frames_with_content:
        try:
            combined_df = pd.concat(all_data_frames_with_content, ignore_index=True)
            logging.info(f"Total de filas de lotes extraídas de {len(all_data_frames_with_content)} subastas: {len(combined_df)}")
            if auctions_with_issues: logging.warning(f"Subastas con problemas ({len(auctions_with_issues)}): {sorted(list(set(auctions_with_issues)))}")
            return combined_df, sorted(list(set(auctions_with_issues)))
        except Exception as e_concat:
             logging.error(f"Error al concatenar DataFrames de lotes: {e_concat}", exc_info=True)
             all_processed_ids = summary_df['auction_id'].tolist() if not summary_df.empty else list(range(start_id, end_id + 1))
             return None, sorted(list(set(all_processed_ids)))
    else:
        logging.error("No se extrajeron datos de lotes válidos de ninguna subasta en el rango.")
        return None, sorted(list(set(auctions_with_issues)))

def process_and_save_data(df_raw, start_id, end_id):
    if df_raw is None or df_raw.empty:
        logging.error("No hay datos brutos para procesar y guardar.")
        return False

    logging.info("--- Iniciando Limpieza y Preparación de Datos para Guardar (Post-Tabula) ---")
    df_cleaned = df_raw.copy()

    # Limpiar archivos anteriores
    try:
        for file in os.listdir(OUTPUT_FOLDER):
            file_path = os.path.join(OUTPUT_FOLDER, file)
            if file.endswith(('.csv', '.xlsx', '.xls')):
                try:
                    os.remove(file_path)
                    logging.info(f"Archivo eliminado: {file}")
                except Exception as e:
                    logging.error(f"Error al eliminar archivo {file}: {e}")
        logging.info("Limpieza de archivos anteriores completada.")
    except Exception as e:
        logging.error(f"Error al limpiar archivos anteriores: {e}")

    if 'FECHA_FERIA' not in df_cleaned.columns or df_cleaned['FECHA_FERIA'].isnull().all():
        logging.error("Columna 'FECHA_FERIA' es esencial o está toda vacía. No se puede continuar.")
        return False
    df_cleaned['FECHA_FERIA'] = pd.to_datetime(df_cleaned['FECHA_FERIA'], errors='coerce')
    df_cleaned.dropna(subset=['FECHA_FERIA'], inplace=True) 
    if df_cleaned.empty:
        logging.error("DataFrame vacío después de procesar FECHA_FERIA.")
        return False
        
    # Asegurarnos que precio base y final usen la misma lógica de miles
    for col_num in ['Cantidad', 'Peso_Total', 'Peso_Promedio']:
        if col_num in df_cleaned.columns:
            df_cleaned[col_num] = pd.to_numeric(df_cleaned[col_num], errors='coerce')

    # Procesar precios con la función clean_currency
    for col_precio in ['Precio_Base', 'Precio_Final']:
        if col_precio in df_cleaned.columns:
            df_cleaned[col_precio] = df_cleaned.apply(
                lambda row: clean_currency(row[col_precio], col_precio, row.get('auction_id'), row.get('Lote')), 
                axis=1
            )

    essential_for_analysis = ['FECHA_FERIA', 'Sexo', 'Cantidad', 'Peso_Total', 'Precio_Final', 'Lote']
    cols_to_dropna = [col for col in essential_for_analysis if col in df_cleaned.columns]
    nan_rows_before_drop = df_cleaned[cols_to_dropna].isnull().any(axis=1).sum() if cols_to_dropna and not df_cleaned.empty else 0
    if nan_rows_before_drop > 0:
        logging.info(f"Se eliminarán {nan_rows_before_drop} filas por NaNs en columnas esenciales para análisis.")
    if cols_to_dropna : df_cleaned.dropna(subset=cols_to_dropna, inplace=True)
    
    for col_num_positive in ['Cantidad', 'Peso_Total', 'Precio_Final']:
        if col_num_positive in df_cleaned.columns: 
            df_cleaned[col_num_positive] = pd.to_numeric(df_cleaned[col_num_positive], errors='coerce') 
            df_cleaned.dropna(subset=[col_num_positive], inplace=True) 
            if not df_cleaned.empty: 
                 df_cleaned = df_cleaned[df_cleaned[col_num_positive] > 0]

    logging.info(f"Filas después de limpieza y filtrado final (para guardar): {len(df_cleaned)}")
    if df_cleaned.empty:
        logging.error("No quedaron datos válidos después de la limpieza final para guardar/agrupar.")
        return False

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Guardar datos limpios en CSV
    clean_filename = os.path.join(OUTPUT_FOLDER, f"{FILENAME_PREFIX_LIMPIOS}_{start_id}_a_{end_id}_{timestamp}.csv")
    try:
        output_columns_clean = [
            'auction_id', 'FERIA_NO', 'FECHA_FERIA', 'Lote', 'Sexo', 'Cantidad',
            'Peso_Total', 'Peso_Promedio', 'Procedencia', 'Hora_Entrada',
            'Precio_Base', 'Precio_Final', 'Observaciones'
        ]
        df_cleaned_output = df_cleaned[[col for col in output_columns_clean if col in df_cleaned.columns]]
        df_cleaned_output.to_csv(clean_filename, index=False, encoding='utf-8-sig', date_format='%Y-%m-%d')
        logging.info(f"Datos limpios guardados en: {clean_filename}")
    except Exception as e_save_clean:
        logging.error(f"Error guardando datos limpios en CSV: {e_save_clean}", exc_info=True)

    logging.info("--- Iniciando Agrupación Semanal ---")
    cols_for_agg = ['FECHA_FERIA', 'Precio_Final', 'Peso_Total', 'Sexo', 'Lote']
    if all(col in df_cleaned.columns for col in cols_for_agg) and not df_cleaned['FECHA_FERIA'].isnull().all():
        try:
            df_for_agg = df_cleaned.copy() 
            df_for_agg['Precio_Final'] = pd.to_numeric(df_for_agg['Precio_Final'], errors='coerce')
            df_for_agg['Peso_Total'] = pd.to_numeric(df_for_agg['Peso_Total'], errors='coerce')
            df_for_agg.dropna(subset=['Precio_Final', 'Peso_Total', 'FECHA_FERIA', 'Sexo', 'Lote'], inplace=True) 

            if not df_for_agg.empty:
                df_for_agg['Valor_Total_Lote'] = df_for_agg['Precio_Final'] * df_for_agg['Peso_Total']
                
                df_agrupado = df_for_agg.groupby([pd.Grouper(key='FECHA_FERIA', freq='W-MON', label='left', closed='left'), 'Sexo']).agg(
                    Valor_Total_Categoria=('Valor_Total_Lote', 'sum'),
                    Peso_Total_Categoria=('Peso_Total', 'sum'),
                    Cantidad_Lotes=('Lote', 'nunique')
                ).reset_index()

                if not df_agrupado.empty:
                    df_agrupado['Precio_Promedio_Ponderado'] = np.where(
                        df_agrupado['Peso_Total_Categoria'] != 0,
                        (df_agrupado['Valor_Total_Categoria'] / df_agrupado['Peso_Total_Categoria']),
                        0 
                    )
                    df_agrupado.replace([np.inf, -np.inf], 0, inplace=True)

                    final_columns_agg = ['FECHA_FERIA', 'Sexo', 'Precio_Promedio_Ponderado', 'Cantidad_Lotes']
                    df_agrupado_final = df_agrupado[[col for col in final_columns_agg if col in df_agrupado.columns]]
                    
                    if not df_agrupado_final.empty:
                        grouped_filename = os.path.join(OUTPUT_FOLDER, f"{FILENAME_PREFIX_AGRUPADOS_SEMANAL}_{start_id}_a_{end_id}_{timestamp}.csv")
                        df_agrupado_final.to_csv(grouped_filename, index=False, encoding='utf-8-sig', float_format='%.2f', date_format='%Y-%m-%d')
                        logging.info(f"Datos agrupados SEMANALMENTE guardados en: {grouped_filename}")
                    else: 
                        logging.warning("Agrupación semanal no produjo datos finales para guardar.")
                else: 
                    logging.warning("DataFrame agrupado vacío después de groupby.")
            else: 
                logging.warning("No hay datos válidos para la agrupación semanal después de la limpieza para 'Valor_Total_Lote'.")
        except Exception as e_agg: 
            logging.error(f"Error durante la agregación semanal: {e_agg}", exc_info=True)
    else:
        missing_cols_agg = [col for col in cols_for_agg if col not in df_cleaned.columns or (col=='FECHA_FERIA' and df_cleaned[col].isnull().all())]
        logging.warning(f"No se pudo realizar la agrupación semanal. Faltan columnas esenciales o todas sus fechas son inválidas. Columnas problemáticas: {missing_cols_agg}")
    return True

# === Ejecución Principal ===
if __name__ == '__main__':
    logging.info("====== INICIO DEL SCRIPT SCRAPER (VERSIÓN TABULA) ======")
    end_id = 1834 
    num_auctions_to_scrape = 120 
    start_id = end_id - num_auctions_to_scrape + 1 
    
    # EJEMPLO PARA PROBAR SOLO LA SUBASTA 1834:
    # start_id = 1834
    # end_id = 1834

    logging.info(f"Se configuró la extracción para las subastas ID {start_id} a {end_id} ({num_auctions_to_scrape} subasta(s)).")
    logging.warning("IMPORTANTE: REVISA Y AJUSTA 'TABLE_AREA_COORDINATES' y MUY ESPECIALMENTE 'COLUMN_X_COORDINATES' en 'parse_pdf_with_tabula'.")
    
    combined_data, auctions_with_issues = scrape_multiple_auctions_tabula(start_id, end_id)

    if combined_data is not None and not combined_data.empty:
        process_and_save_data(combined_data, start_id, end_id)
    else:
        logging.error("El scraping no produjo datos combinados de lotes válidos para procesar y guardar.")

    if auctions_with_issues:
        logging.warning(f"Subastas que tuvieron problemas (errores o sin datos de lotes) ({len(auctions_with_issues)}): {auctions_with_issues}")
    else:
        if combined_data is not None and not combined_data.empty :
             logging.info("Todas las subastas en el rango se procesaron y se extrajeron datos de lotes.")
        elif combined_data is None or combined_data.empty: 
             logging.info("Se procesaron subastas pero no se extrajo ningún dato de lotes de ninguna de ellas.")

    logging.info("====== FIN DEL SCRIPT SCRAPER (VERSIÓN TABULA) ======")