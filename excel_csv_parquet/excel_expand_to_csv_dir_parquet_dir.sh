#!/bin/bash

####################################################################
######## SETUP
####################################################################

# Verificar que se haya proporcionado el nombre del archivo
if [ -z "$1" ]; then
    echo "Uso: $0 <nombre_del_archivo.xlsx>"
    exit 1
fi

input_file="$1"

####################################################################
######## EXTRACCIÓN NOMBRES HOJAS
####################################################################

# Crear carpeta temporal para unzippear
temp_dir=$(mktemp -d)

# Descomprimir el xlsx
unzip -q "$input_file" -d "$temp_dir"

if [ ! -f "$temp_dir/xl/workbook.xml" ]; then
  echo "No se encontró el archivo workbook.xml"
  rm -rf "$temp_dir"
  exit 1
fi

# Extraer nombres reales de hojas (manteniendo nombres completos)
sheet_names=$(grep -oP 'name="[^"]+"' "$temp_dir/xl/workbook.xml" | sed -E 's/name="(.+)"/\1/' | grep -v '^microsoft\.com:' | grep -v '^_xlnm\.')

rm -rf "$temp_dir"

echo "Hojas encontradas:"
index=1
declare -A sheet_map
while read -r sheet; do
  echo "Hoja $index: $sheet"
  sheet_map[$index]="$sheet"
  ((index++))
done <<< "$sheet_names"
echo ""

####################################################################
######## EXTRACCIÓN HOJAS A CSV
####################################################################

output_dir="hojas_expandidas"
mkdir -p "$output_dir"

echo "Iniciando extracción de hojas de '$input_file' a CSV en '$output_dir'..."

start_total=$(date +%s)

echo "Intentando extraer todas las hojas en un solo paso..."
output=$(xlsx2csv --outputencoding latin1 -s 0 "$input_file" "$output_dir" 2>&1)

if [[ $? -ne 0 ]]; then
    echo "Extracción en un solo paso fallida. Procediendo hoja por hoja..."

    sheet_index=1

    while true; do
        sheet_name="${sheet_map[$sheet_index]}"
        if [ -z "$sheet_name" ]; then
            echo "Proceso completado. Total de hojas extraídas: $((sheet_index - 1))"
            break
        fi

        sanitized_name=$(echo "$sheet_name" | tr ' /' '_' | tr -d '()')
        csv_file="${output_dir}/hoja_${sheet_index}_${sanitized_name}.csv"

        echo "Extrayendo hoja $sheet_index: '$sheet_name' a '$csv_file'..."
        output=$(xlsx2csv --outputencoding latin1 -s "$sheet_index" "$input_file" "$csv_file" 2>&1)
        exit_code=$?

        if [[ $exit_code -ne 0 ]]; then
            echo "Error al procesar la hoja $sheet_index: $output"
            ((sheet_index++))
            continue
        fi

        ((sheet_index++))
    done
else
    echo "Extracción en un solo paso completada."
fi

end_total=$(date +%s)
total_time=$((end_total - start_total))
total_minutes=$((total_time / 60))
total_seconds=$((total_time % 60))

echo "Extracción de CSV completa en $total_minutes minutos y $total_seconds segundos."
echo ""

####################################################################
######## CONVERSIÓN A PARQUET
####################################################################

parquet_dir="hojas_expandidas_parquet"
mkdir -p "$parquet_dir"

echo "Iniciando conversión a Parquet en '$parquet_dir'..."

# Función para detectar encoding
detect_encoding() {
    local file="$1"
    # Detectar encoding usando file
    encoding=$(file -bi "$file" | awk -F "=" '{print $2}')
    echo "$encoding"
}

for csv_file in "$output_dir"/*.csv; do
    base_name=$(basename "$csv_file" .csv)
    parquet_file="${parquet_dir}/${base_name}.parquet"

    # Detectar encoding
    encoding=$(detect_encoding "$csv_file")
    echo "Encoding detectado en '$csv_file': $encoding"

    # Archivo temporal para conversión
    temp_file="${output_dir}/temp_${base_name}.csv"

    if [ "$encoding" != "utf-8" ]; then
        echo "Convirtiendo '$csv_file' de $encoding a utf-8..."
        iconv -f "$encoding" -t utf-8 "$csv_file" -o "$temp_file"
        if [ $? -ne 0 ]; then
            echo "Error al convertir '$csv_file'. Saltando..."
            continue
        fi
    else
        cp "$csv_file" "$temp_file"
    fi

    # Convertir a Parquet
    echo "Convirtiendo '$temp_file' a Parquet..."
    csv2parquet "$temp_file" --output "$parquet_file"

    if [[ $? -ne 0 ]]; then
        echo "Error al convertir '$temp_file' a Parquet."
    else
        echo "Archivo convertido a '$parquet_file'."
    fi

    # Eliminar archivo temporal
    rm "$temp_file"
done

end_total=$(date +%s)
total_time=$((end_total - start_total))
total_minutes=$((total_time / 60))
total_seconds=$((total_time % 60))

echo "Conversión a Parquet completa en $total_minutes minutos y $total_seconds segundos."
