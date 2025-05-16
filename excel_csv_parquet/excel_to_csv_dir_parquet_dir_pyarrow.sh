#!/bin/bash

####################################################################
######## SETUP
####################################################################

if [ -z "$1" ]; then
  echo "Uso: $0 <archivo.xlsx>"
  exit 1
fi

input_file="$1"
output_dir="hojas_expandidas"
parquet_dir="hojas_expandidas_parquet"
delimiter=$'\t'  # Tabulador seguro

mkdir -p "$output_dir"
mkdir -p "$parquet_dir"

####################################################################
######## EXTRACCIÓN NOMBRES HOJAS
####################################################################

temp_dir=$(mktemp -d)
unzip -q "$input_file" -d "$temp_dir"

if [ ! -f "$temp_dir/xl/workbook.xml" ]; then
  echo "❌ No se encontró workbook.xml"
  rm -rf "$temp_dir"
  exit 1
fi

sheet_names=$(grep -oP 'name="[^"]+"' "$temp_dir/xl/workbook.xml" | sed -E 's/name="(.+)"/\1/' | grep -v '^microsoft\.com:' | grep -v '^_xlnm\.')
rm -rf "$temp_dir"

declare -A sheet_map
index=1
echo "📄 Hojas encontradas:"
while read -r sheet; do
  echo "  [$index] $sheet"
  sheet_map[$index]="$sheet"
  ((index++))
done <<< "$sheet_names"
echo ""

####################################################################
######## EXTRACCIÓN A CSV (TAB-DELIMITED)
####################################################################

echo "📤 Extrayendo hojas a CSV en '$output_dir'..."

for i in "${!sheet_map[@]}"; do
  name="${sheet_map[$i]}"
  clean_name=$(echo "$name" | tr ' /' '_' | tr -d '()')
  csv_file="${output_dir}/hoja_${i}_${clean_name}.tsv"

  echo "  → Hoja $i: '$name' → $csv_file"

  xlsx2csv -d "$delimiter" --outputencoding utf-8 -s "$i" "$input_file" "$csv_file"

  if [[ $? -ne 0 ]]; then
    echo "  ⚠️  Error al extraer hoja $i, se omite."
    continue
  fi
done

####################################################################
######## CONVERSIÓN A PARQUET (R Compatible)
####################################################################

echo ""
echo "📦 Convirtiendo TSV a Parquet en '$parquet_dir' (formato R compatible)..."

for tsv_file in "$output_dir"/*.tsv; do
  [[ ! -f "$tsv_file" ]] && continue

  base_name=$(basename "$tsv_file" .tsv)
  parquet_file="${parquet_dir}/${base_name}.parquet"

  echo "  → $tsv_file → $parquet_file"

  # Validación de estructura
  expected_cols=$(head -n 1 "$tsv_file" | awk -F"$delimiter" '{print NF}')
  bad_rows=$(awk -F"$delimiter" -v n=$expected_cols 'NF != n {print NR}' "$tsv_file" | wc -l)

  if [[ "$bad_rows" -gt 0 ]]; then
    echo "  ❌ $tsv_file tiene $bad_rows filas mal estructuradas. Saltando."
    continue
  fi

  # Conversión con pyarrow para compatibilidad R
  python3 - <<END
import pyarrow.csv as pv
import pyarrow.parquet as pq

try:
    table = pv.read_csv(
        "$tsv_file",
        read_options=pv.ReadOptions(encoding='utf-8'),
        parse_options=pv.ParseOptions(delimiter='\t')
    )
    pq.write_table(
        table,
        "$parquet_file",
        version="1.0",
        use_dictionary=False,
        compression="snappy",
        flavor="spark"
    )
except Exception as e:
    print("  ❌ Error en '$tsv_file':", e)
END

done

####################################################################
######## VALIDACIÓN FINAL
####################################################################

echo ""
echo "📊 Resumen de archivos CSV:"
for f in "$output_dir"/*.tsv; do
  [[ -f "$f" ]] || continue
  rows=$(wc -l < "$f")
  cols=$(head -n 1 "$f" | awk -F"$delimiter" '{print NF}')
  echo "  🟢 $(basename "$f"): $rows filas, $cols columnas"
done

echo ""
echo "📊 Resumen de archivos Parquet:"
for f in "$parquet_dir"/*.parquet; do
  [[ -f "$f" ]] || continue
  python3 - <<END
import pyarrow.parquet as pq
try:
    t = pq.read_table("$f")
    print("  🟢 $f: {} filas, {} columnas".format(t.num_rows, t.num_columns))
except Exception as e:
    print("  ❌ Error leyendo $f:", e)
END
done
