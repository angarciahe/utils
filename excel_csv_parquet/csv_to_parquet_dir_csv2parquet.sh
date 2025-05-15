#!/bin/bash

# Verificar los argumentos
if [ $# -ne 1 ]; then
  echo "Uso: $0 <archivo.xlsx>"
  exit 1
fi

# Archivo de entrada
input_dir="$1"

#!/bin/bash

# Carpeta de entrada y salida
output_dir="$(basename "$input_dir")_parquets"

# Crear carpeta de salida si no existe
mkdir -p "$output_dir"

# Recorrer los archivos CSV en la carpeta de entrada
for csv_file in "$input_dir"/*.csv; do
  # Obtener el nombre base del archivo sin extensión
  base_name=$(basename "$csv_file" .csv)

  # Definir el archivo de salida con el sufijo _parquet
  parquet_file="${output_dir}/${base_name}_parquet.parquet"

  echo "Convirtiendo '$csv_file' a '$parquet_file'..."
  
  # Convertir CSV a Parquet
  csv2parquet "$csv_file" --output "$parquet_file"

  # Verificar si hubo algún error
  if [[ $? -ne 0 ]]; then
    echo "Error al convertir '$csv_file'."
  else
    echo "Archivo convertido: '$parquet_file'"
  fi
done

echo "Proceso completado. Archivos generados en $output_dir"
