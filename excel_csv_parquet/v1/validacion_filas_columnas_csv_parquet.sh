#!/bin/bash

# Verificar que se haya proporcionado al menos un archivo
if [ $# -lt 1 ]; then
  echo "Uso: $0 <archivo1> [archivo2 ... archivoN]"
  exit 1
fi

for file in "$@"; do
    echo "Procesando: $file"

    # Obtener la extensión del archivo
    extension="${file##*.}"

    # Verificar si es CSV
    if [[ "$extension" == "csv" ]]; then
        # Detectar el encoding
        encoding=$(file -bi "$file" | awk -F "=" '{print $2}')
        echo "Encoding detectado: $encoding"

        # Obtener el número de filas (excluyendo el header) y columnas
        num_columns=$(head -n 1 "$file" | sed 's/[^,]//g' | wc -c)
        num_rows=$(wc -l < "$file")
        echo "Tipo: CSV"
        echo "Columnas: $num_columns"
        echo "Filas: $((num_rows - 1))"  # Restar 1 para no contar el header

    # Verificar si es Parquet
    elif [[ "$extension" == "parquet" ]]; then
        # Usar parquet-tools para obtener información
        if ! command -v parquet-tools &> /dev/null; then
            echo "Error: parquet-tools no está instalado. Ejecuta 'sudo apt install parquet-tools' para instalarlo."
            continue
        fi

        # Obtener número de filas y columnas
        num_rows=$(parquet-tools rowcount "$file" 2>/dev/null)
        num_columns=$(parquet-tools schema "$file" 2>/dev/null | grep -c ": ")
        
        echo "Tipo: Parquet"
        echo "Columnas: $num_columns"
        echo "Filas: $num_rows"
    else
        echo "Tipo de archivo no soportado: $file"
    fi

    echo "---------------------------"
done
