#!/bin/bash

if [ -z "$1" ]; then
  echo "Uso: $0 archivo.xlsx"
  exit 1
fi

file="$1"
temp_dir=$(mktemp -d)

unzip -q "$file" -d "$temp_dir"

if [ ! -f "$temp_dir/xl/workbook.xml" ]; then
  echo "No se encontró el archivo workbook.xml"
  rm -rf "$temp_dir"
  exit 1
fi

echo "Nombres de hojas reales en '$file':"

# Inicializar índice
index=1

# Iterar sobre los nombres de las hojas y mostrarlos con el índice
grep -oP 'name="\K[^"]+' "$temp_dir/xl/workbook.xml" | grep -v '^microsoft\.com:' | grep -v '^_xlnm\.' | while read -r sheet_name; do
  echo "Hoja $index: $sheet_name"
  ((index++))
done

rm -rf "$temp_dir"
