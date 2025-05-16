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

sheet_index=1

while true; do
    # Verificar si el índice existe en el mapa de hojas
    sheet_name="${sheet_map[$sheet_index]}"
    if [ -z "$sheet_name" ]; then
        echo "Proceso completado. Total de hojas extraídas: $((sheet_index - 1))"
        break
    fi

    # Reemplazar espacios y caracteres especiales en el nombre del archivo
    sanitized_name=$(echo "$sheet_name" | tr ' /' '_' | tr -d '()')

    # Definir el nombre del archivo
    output_file="${output_dir}/hoja_${sheet_index}_${sanitized_name}.csv"

    start_time=$(date +%s)

    # Primer intento sin --ignore-format
    echo "Extrayendo hoja $sheet_index: '$sheet_name' a '$output_file'..."
    output=$(xlsx2csv -s "$sheet_index" "$input_file" "$output_file" 2>&1)
    exit_code=$?

    # Si falla, intentar con --ignore-format
    if [[ $exit_code -ne 0 ]]; then
        if [[ $output == *"not found or can't be handled"* ]]; then
            echo "Proceso completado. Total de hojas extraídas: $((sheet_index - 1))"
            break
        fi

        if [[ $output == *"could not convert string to float:"* ]]; then
            echo "Error de conversión float detectado en hoja $sheet_index, reintentando con --ignore-format float..."
            output=$(xlsx2csv --ignore-format float -s "$sheet_index" "$input_file" "$output_file" 2>&1)
            exit_code=$?

            if [[ $exit_code -ne 0 ]]; then
                echo "Reintento fallido en hoja $sheet_index: $output"
                echo "Continuando con la siguiente hoja..."
                ((sheet_index++))
                continue
            fi
        else
            echo "Error al procesar la hoja $sheet_index: $output"
            echo "Continuando con la siguiente hoja..."
            ((sheet_index++))
            continue
        fi
    fi

    end_time=$(date +%s)
    elapsed_time=$((end_time - start_time))
    minutes=$((elapsed_time / 60))
    seconds=$((elapsed_time % 60))

    echo "Hoja $sheet_index ('${sheet_name}') extraída en $minutes minutos y $seconds segundos."

    ((sheet_index++))
done

end_total=$(date +%s)
total_time=$((end_total - start_total))
total_minutes=$((total_time / 60))
total_seconds=$((total_time % 60))

echo "Proceso completo en $total_minutes minutos y $total_seconds segundos."

echo "Archivos CSV generados en '$output_dir':"
ls -1 "$output_dir"
