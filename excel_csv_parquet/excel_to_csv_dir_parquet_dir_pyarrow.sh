#!/bin/bash

if [ -z "$1" ]; then
  echo "Uso: $0 <archivo.xlsx>"
  exit 1
fi

input_file="$1"
output_dir="hojas_expandidas"
parquet_dir="hojas_expandidas_parquet"
delimiter=$'\t'

mkdir -p "$output_dir"
mkdir -p "$parquet_dir"

# Obtener nombres de hojas
temp_dir=$(mktemp -d)
unzip -q "$input_file" -d "$temp_dir"
sheet_names=$(grep -oP 'name="[^"]+"' "$temp_dir/xl/workbook.xml" |
  sed -E 's/name="(.+)"/\1/' |
  grep -v '^microsoft\.com:' |
  grep -v '^_xlnm\.')
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

# Extracción TSV
echo "📤 Extrayendo hojas a archivos TSV (UTF-8)..."

for i in "${!sheet_map[@]}"; do
  name="${sheet_map[$i]}"
  clean_name=$(echo "$name" | tr ' /' '_' | tr -d '()')
  tsv_file="${output_dir}/hoja_${i}_${clean_name}.tsv"

  echo "  → Hoja $i: '$name' a $tsv_file"

  output=$(xlsx2csv -d "$delimiter" --outputencoding utf-8 -s "$i" "$input_file" "$tsv_file" 2>&1)
  exit_code=$?

  if [[ $exit_code -ne 0 && "$output" == *"could not convert string to float"* ]]; then
    echo "    ⚠️ Error float. Reintentando como texto..."
    xlsx2csv -d "$delimiter" --ignore-format float --outputencoding utf-8 -s "$i" "$input_file" "$tsv_file"
    [[ $? -ne 0 ]] && echo "    ❌ Fallo hoja $i. Se omite." && continue
  elif [[ $exit_code -ne 0 ]]; then
    echo "    ❌ Fallo hoja $i. Se omite."
    continue
  fi
done

# Conversión TSV → Parquet con validación estructural
echo ""
echo "📦 Validando y convirtiendo a Parquet..."

for tsv_file in "$output_dir"/*.tsv; do
  [[ -f "$tsv_file" ]] || continue

  base_name=$(basename "$tsv_file" .tsv)
  ok_file="${output_dir}/${base_name}_ok.tsv"
  bad_file="${output_dir}/${base_name}_mala_estructura.tsv"
  parquet_file="${parquet_dir}/${base_name}_ok.parquet"

  # Detectar número esperado de columnas
  expected_cols=$(head -n 1 "$tsv_file" | awk -F"$delimiter" '{print NF}')

  # Separar válidas e inválidas
  head -n 1 "$tsv_file" > "$ok_file"
  head -n 1 "$tsv_file" > "$bad_file"

  awk -F"$delimiter" -v n="$expected_cols" 'NR > 1 { 
    if (NF == n) print >> "'"$ok_file"'"; 
    else print >> "'"$bad_file"'"; 
  }' "$tsv_file"

  ok_rows=$(wc -l < "$ok_file")
  bad_rows=$(($(wc -l < "$bad_file") - 1))

  if [[ "$ok_rows" -le 1 ]]; then
    echo "  ❌ $base_name: todas las filas están mal estructuradas. No se genera Parquet."
    continue
  fi

  [[ "$bad_rows" -gt 0 ]] && echo "  ⚠️ $bad_rows filas mal estructuradas guardadas en: $bad_file"

  # Convertir archivo limpio a Parquet
  echo "  ✅ Convirtiendo $ok_file → $parquet_file"
  python3 - <<END
import pyarrow.csv as pv
import pyarrow.parquet as pq

try:
    table = pv.read_csv(
        "$ok_file",
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
    print("  ❌ Error en conversión de '$ok_file':", e)
END
done

# Validación final
echo ""
echo "📊 Resumen TSV válidos:"
for f in "$output_dir/"*_ok.tsv; do
  [[ -f "$f" ]] || continue
  rows=$(wc -l < "$f")
  cols=$(head -n 1 "$f" | awk -F"$delimiter" '{print NF}')
  echo "  🟢 $(basename "$f"): $rows filas, $cols columnas"
done

echo ""
echo "📊 Resumen Parquet:"
for f in "$parquet_dir/"*_ok.parquet; do
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
