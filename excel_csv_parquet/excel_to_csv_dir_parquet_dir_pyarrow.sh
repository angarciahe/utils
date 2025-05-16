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

output_dir="hojas_expandidas"
parquet_dir="hojas_expandidas_parquet"

mkdir -p "$output_dir"
mkdir -p "$parquet_dir"

echo "Procesando archivo: $input_file"
echo ""

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

start_total=$(date +%s)

echo "Iniciando extracción de hojas a CSV en '$output_dir'..."

sheet_index=1
while true; do
    sheet_name="${sheet_map[$sheet_index]}"

    if [ -z "$sheet_name" ]; then
        echo "Proceso de extracción CSV completado."
        break
    fi

    sanitized_name=$(echo "$sheet_name" | tr ' /' '_' | tr -d '()')
    csv_file="${output_dir}/hoja_${sheet_index}_${sanitized_name}.csv"

    # Verificar si hay tildes o caracteres especiales para definir el encoding
    has_special_chars=$(echo "$sheet_name" | grep -E '[ñÑáéíóúÁÉÍÓÚ]')
    if [ -n "$has_special_chars" ]; then
        encoding="latin1"
    else
        encoding="utf-8"
    fi

    echo "Extrayendo hoja $sheet_index ('$sheet_name') con encoding $encoding a '$csv_file'..."

    # Extracción con el encoding adecuado
    output=$(xlsx2csv --outputencoding "$encoding" -s "$sheet_index" "$input_file" "$csv_file" 2>&1)
    exit_code=$?

    if [[ $exit_code -ne 0 ]]; then
        if [[ $output == *"not found or can't be handled"* ]]; then
            echo "Proceso completado. Total de hojas extraídas: $((sheet_index - 1))"
            break
        fi

        if [[ $output == *"could not convert string to float:"* ]]; then
            echo "Error de conversión float detectado en hoja $sheet_index. Reintentando con --ignore-format float..."
            output=$(xlsx2csv --ignore-format float --outputencoding "$encoding" -s "$sheet_index" "$input_file" "$csv_file" 2>&1)
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

    echo "Hoja $sheet_index ('${sheet_name}') extraída exitosamente."

    ((sheet_index++))
done

end_total=$(date +%s)
elapsed_time=$((end_total - start_total))
echo "Extracción a CSV completada en $((elapsed_time / 60)) minutos y $((elapsed_time % 60)) segundos."
echo ""

####################################################################
######## CONVERSIÓN A PARQUET
####################################################################

echo "Iniciando conversión a Parquet en '$parquet_dir'..."

# Función para detectar tildes y ñ
detect_encoding() {
    local file="$1"
    if grep -qP '[áéíóúÁÉÍÓÚñÑ]' "$file"; then
        echo "latin1"
    else
        echo "utf-8"
    fi
}

start_total=$(date +%s)

for csv_file in "$output_dir"/*.csv; do
    base_name=$(basename "$csv_file" .csv)
    parquet_file="${parquet_dir}/${base_name}_parquet.parquet"

    # Determinar encoding
    encoding=$(detect_encoding "$csv_file")

    # Si el encoding es latin1, convertir temporalmente a utf-8
    if [ "$encoding" == "latin1" ]; then
        temp_file="${output_dir}/temp_${base_name}.csv"
        echo "Convirtiendo '$csv_file' de latin1 a utf-8..."
        iconv -f latin1 -t utf-8 "$csv_file" -o "$temp_file"
        csv_file="$temp_file"
    fi

    # Convertir a Parquet
    echo "Convirtiendo '$csv_file' a Parquet..."
    csv2parquet "$csv_file" --output "$parquet_file"

    if [[ $? -ne 0 ]]; then
        echo "Error al convertir '$csv_file' a Parquet."
    else
        echo "Archivo convertido a '$parquet_file'."
    fi

    # Eliminar archivo temporal
    if [[ -f "$temp_file" ]]; then
        rm "$temp_file"
    fi
done

end_total=$(date +%s)
elapsed_time=$((end_total - start_total))
echo "Conversión a Parquet completada en $((elapsed_time / 60)) minutos y $((elapsed_time % 60)) segundos."
echo ""
