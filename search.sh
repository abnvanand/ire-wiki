if [[ $# -ne 3 ]]; then
	echo "Pass exactly 3 args <path_to_index_folder> <path_to_input_query_file> <path_to_output_file>.";
	exit 1;
fi

path_to_index_folder=$1;
path_to_input_query_file=$2;
path_to_output_file=$3;

python search.py "$path_to_index_folder" "$path_to_input_query_file" "$path_to_output_file"
