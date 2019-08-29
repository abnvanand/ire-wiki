if [[ $# -ne 2 ]]; then
        echo "Pass exactly 2 args <path_to_dump> and <path_to_index_folder>";
        exit 1;
fi
path_to_dump=$1;
path_to_index_folder=$2;
echo "$path_to_dump";
echo "$path_to_index_folder";
mkdir "$path_to_index_folder"
python build_index.py "$path_to_dump" "$path_to_index_folder"
