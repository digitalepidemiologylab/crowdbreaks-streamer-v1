echo "Running download binaries script..."

# add files to array here
files=( "./web/bin/vaccine_sentiment/fasttext_v1.ftz" )
urls=( "https://s3.eu-central-1.amazonaws.com/crowdbreaks-dev/binaries/fasttext_v1.ftz" )

for ((i = 0; i < ${#files[@]}; ++i)); do
  if [ ! -f ${files[$i]} ]; then 
    echo "Downloading file ${files[$i]}..."
    curl "${urls[$i]}" -o "${files[$i]}" --create-dirs
    if [ -e ${files[$i]} ]; then 
      echo "Success!"
    fi;
  fi;
done

