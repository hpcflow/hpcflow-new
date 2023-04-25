BASH_SHEBANG = """#!{shell_executable} {shell_args}"""

BASH_HEADER = """
{workflow_app_alias} () {{
  (
{env_setup}{app_invoc}\\
    --config-dir "{config_dir}"\\
    --config-invocation-key "{config_invoc_key}"\\
    "$@"
  )
}}

WK_PATH=`pwd`
SUB_IDX={sub_idx}
JS_IDX={js_idx}
EAR_ID_FILE="$WK_PATH/submissions/${{SUB_IDX}}/{EAR_file_name}"
ELEM_RUN_DIR_FILE="$WK_PATH/submissions/${{SUB_IDX}}/{element_run_dirs_file_path}"
"""

BASH_SCHEDULER_HEADER = """{bash_shebang}

{scheduler_options}
{bash_header}
"""

BASH_DIRECT_HEADER = """{bash_shebang}

{bash_header}
"""

BASH_MAIN = """
elem_need_EARs=`sed "${{JS_elem_idx}}q;d" $EAR_ID_FILE`
elem_run_dirs=`sed "${{JS_elem_idx}}q;d" $ELEM_RUN_DIR_FILE`

for JS_act_idx in {{1..{num_actions}}}
do

  need_EAR="$(cut -d'{EAR_files_delimiter}' -f $JS_act_idx <<< $elem_need_EARs)"
  if [ "$need_act" = "0" ]; then
      continue
  fi

  run_dir="$(cut -d'{EAR_files_delimiter}' -f $JS_act_idx <<< $elem_run_dirs)"
  cd $WK_PATH/$run_dir

  {workflow_app_alias} internal workflow $WK_PATH write-commands $SUB_IDX $JS_IDX $(($JS_elem_idx - 1)) $(($JS_act_idx - 1))
  {workflow_app_alias} internal workflow $WK_PATH set-ear-start $SUB_IDX $JS_IDX $(($JS_elem_idx - 1)) $(($JS_act_idx - 1))
  . {commands_file_name}
  {workflow_app_alias} internal workflow $WK_PATH set-ear-end $SUB_IDX $JS_IDX $(($JS_elem_idx - 1)) $(($JS_act_idx - 1))

done
"""

BASH_ELEMENT_LOOP = """for JS_elem_idx in {{1..{num_elements}}}
do
{bash_main}
done
"""

BASH_ELEMENT_ARRAY = """JS_elem_idx=${scheduler_array_item_var}
{bash_main}
"""
