#!/usr/bin/env bash

cd "$( dirname "${BASH_SOURCE[0]}" )/.."

overlay_mount() {
  real_app_path="$1"
  # Hot upgrade service pkg, for service pkg, implement overlay mount
  if [[ -e "$real_app_path" && "$real_app_path" != "/app" ]]; then
    dir_part=$(dirname "$real_app_path") 
    file_part=$(basename "$real_app_path")
    overlay_mount_shell="${dir_part}/.layers_${file_part}/${file_part}/.hot_upgrade_meta/overlay_mount.sh"
    # 执行挂载的前提条件，挂载脚本存在，且目标目录没被挂载过
    if [ -f "$overlay_mount_shell" ] && ! mountpoint -q "${real_app_path}"; then
      mount_res=$(bash "${overlay_mount_shell}")
      check_code=$?
      if [[ ! $check_code -eq 0 ]]; then
        echo "[app_path_switch] ${current_date} overlay mount failed! ret: ${check_code}, detail:${mount_res}" >> "${APP_PATH_SWITCH_LOG}"
      else
        echo "[app_path_switch] ${current_date} overlay mount success for ${real_app_path}" >> "${APP_PATH_SWITCH_LOG}"
      fi
    fi
  fi
}

CUR_APP_RUN_PATH="/userdata/mnt/hot_upgrade/storage/.app_path"
NEXT_APP_RUN_PATH="/userdata/mnt/hot_upgrade/storage/.next_app_path"
APP_RESTART_TAG="/tmp/hot_upgrade_restart_app_request"
BASE_ORIN_LOG_PATH="/applog/applog/orin_log" # Default orin log path

# Overwrite if path exist
if [[ -e "/dataloop1/applog/orin_log" ]]; then
  BASE_ORIN_LOG_PATH="/dataloop1/applog/orin_log"
fi

APP_PATH_SWITCH_LOG="${BASE_ORIN_LOG_PATH}/app_upgrade_mgr.log"

mkdir -p /userdata/mnt/hot_upgrade/storage
mkdir -p /userdata/mnt/hot_upgrade/runtime/A
mkdir -p /userdata/mnt/hot_upgrade/runtime/B
mkdir -p "${BASE_ORIN_LOG_PATH}"
mkdir -p /run/app

current_date=$(date +"%Y-%m-%d %H:%M:%S")

if [[ ! -e "$CUR_APP_RUN_PATH" ]] && [[ ! -e "$NEXT_APP_RUN_PATH" ]]; then # 如果.app_path和.next_app_path不存在，直接写入/app根目录到该文件里
  echo "/app" > "${CUR_APP_RUN_PATH}"
  echo "[app_path_switch] ${current_date} reset cur app path to /app" >> "${APP_PATH_SWITCH_LOG}"
  python3 modules/applications/hot_upgrade/script/app_mgr.py -c check_app_path -p 0
elif [[ -f "$NEXT_APP_RUN_PATH" ]]; then # 如果.next_app_path存在，则作为本次要运行的目录
  ready_path="$(cat ${NEXT_APP_RUN_PATH})"
  mv "${NEXT_APP_RUN_PATH}" "${CUR_APP_RUN_PATH}"
  overlay_mount "${ready_path}"
  echo "[app_path_switch] ${current_date} mv next app path ${ready_path} to cur app path" >> "${APP_PATH_SWITCH_LOG}"
  python3 modules/applications/hot_upgrade/script/app_mgr.py -c check_app_path -p 1
else # 如果.next_app_path不存在且.app_path存在
  ready_path="$(cat ${CUR_APP_RUN_PATH})"
  overlay_mount "${ready_path}"
  echo "[app_path_switch] ${current_date} directly run app_path ${ready_path}" >> "${APP_PATH_SWITCH_LOG}"
  if [[ -f "$APP_RESTART_TAG" ]]; then # 说明是服务自检没通过的异常场景
    echo "[app_path_switch] ${current_date} restart_app scenario, will check and revert to new app path" >> "${APP_PATH_SWITCH_LOG}"
    python3 modules/applications/hot_upgrade/script/app_mgr.py -c check_app_path -p 1
  else
    python3 modules/applications/hot_upgrade/script/app_mgr.py -c check_app_path -p 0
  fi
fi
