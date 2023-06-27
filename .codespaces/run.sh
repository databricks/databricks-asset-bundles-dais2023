if [ ! -d /home/codespace/.vscode-remote/extensions/databricks.databricks-1.0.0-linux-x64 ]; then
  echo "Please install the Databricks VS Code extension first..."
  return
fi

export PS1='$ '
export PATH=/home/codespace/.vscode-remote/extensions/databricks.databricks-1.0.0-linux-x64/bin/:$PATH

databricks -v
