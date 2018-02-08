filename='../secrets.list'
while read line; do    
  regex="^[A-Z]=[:print:]$"
  regex="^[[:print: ]]$"
  if [[ $line =~ = ]]; then
    IFS='=' read -a env <<< "$line"
    echo "Setting env ${env[0]} to ${env[1]}"
    eval export ${env[0]}=${env[1]}
  fi
done < "$filename"
