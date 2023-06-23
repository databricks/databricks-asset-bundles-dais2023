if [ ! -d /home/codespace/.vscode-remote/extensions/databricks.databricks-0.3.15-linux-x64 ]; then
  echo "Please install the Databricks VS Code extension first..."
  return
fi

export PS1='$ '
export PATH=/home/codespace/.vscode-remote/extensions/databricks.databricks-0.3.15-linux-x64/bin/:$PATH

databricks -v
