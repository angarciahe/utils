# Detectar el encoding
encoding=$(file -bi "$input_file" | awk -F "=" '{print $2}')

# Si el encoding no es UTF-8, cambia a latin1
if [[ "$encoding" != "utf-8" ]]; then
    encoding="latin1"
else
    encoding="utf-8"
fi

echo "Encoding detectado para '$input_file': $encoding"
