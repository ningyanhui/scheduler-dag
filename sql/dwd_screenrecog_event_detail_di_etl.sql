set mapreduce.job.name=dwd_screenrecog_event_detail_di_etl;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table tranai_dw.dwd_screenrecog_event_detail_di partition(p_date, insert_date)
select 
    a0.tid as event_tid,
    a0.event as event_name,
    -- 公共属性字段
    a0.device_id,
    a0.gaid,
    a0.rom_version,
    a0.os_version,
    a0.device_memory,
    a0.sys_lg,
    a0.country_code,
    a0.device_model,
    a0.device_brand,
    a0.product_line,
    a0.app_version_fmt,
    a0.app_vercode,
    a0.app_vername,
    a0.app_package_name,
    a0.network_type,
    a0.net_status,
    a0.net,
    a0.event_timestamp,
    a0.event_time,
    a0.ts,
    a0.tz,
    a0.sts,
    a0.wk_dt,
    a0.bj_dt,
    a0.iso,
    a0.ctime,
    a0.public_params,
    -- 事件属性字段
    a0.card_rank,
    a0.card_title,
    a0.click_result,
    a0.click_type,
    a0.dur_time,
    a0.element_type,
    a0.enter_app,
    a0.enter_type,
    a0.fail_reason,
    a0.fail_result,
    a0.from_type,
    a0.is_oriented,
    a0.ocr_language,
    a0.ocr_nums,
    a0.ocr_result,
    a0.on_off,
    a0.oriented_type,
    a0.page_type,
    a0.params,
    a0.reason,
    a0.scene_app,
    a0.select_type,
    a0.setting_info,
    a0.setting_page_type,
    a0.setting_type,
    a0.stage,
    a0.stay_time,
    a0.target_version,
    a0.task_download_type,
    a0.task_id,
    a0.task_name,
    a0.task_product_name,
    a0.text_content,
    a0.toast_content,
    a0.upgrade_from,
    a0.upgrade_type,
    a0.upload_status,
    
    -- ETL处理时间
    current_timestamp() as etl_time,
    
    -- 动态分区
    a0.p_date,
    '${day_id}' as insert_date

from (
    select 
        tid,
        event,
        -- 公共属性字段
        nvl(get_json_object(eparam, '$.__vd'), '') as device_id,
        nvl(get_json_object(eparam, '$._gaid'), '') as gaid,
        nvl(get_json_object(eparam, '$._rom_version'), '') as rom_version,
        nvl(get_json_object(eparam, '$._os_version'), '') as os_version,
        nvl(get_json_object(eparam, '$._device_memory'), '') as device_memory,
        nvl(get_json_object(eparam, '$._sys_language'), '') as sys_lg,
        nvl(get_json_object(eparam, '$._country_code'), '') as country_code,
        nvl(get_json_object(eparam, '$._device_model'), lower(device_fmt)) as device_model,
        nvl(get_json_object(eparam, '$._device_brand'), lower(brand)) as device_brand,
        nvl(get_json_object(eparam, '$._product_line'), '') as product_line,
        case when get_json_object(eparam, '$._app_vername') > ''
                then get_app_version_fmt_by_vername(get_json_object(eparam, '$._app_vername'))
            when get_json_object(eparam,'$._app_vercode') > ''
                then get_app_version_fmt_by_vercode(get_json_object(eparam,'$._app_vercode'))
            when get_json_object(eparam, '$.__ver') > ''  --用trancareSDK版本兜底
                then get_app_version_fmt_by_ver(get_json_object(eparam, '$.__ver'))
            else ''
        end as app_version_fmt,
        nvl(get_json_object(eparam, '$._app_vercode'), '') as app_vercode,
        nvl(get_json_object(eparam, '$._app_vername'), '') as app_vername,
        nvl(get_json_object(eparam, '$._app_package_name'), '') as app_package_name,
        nvl(get_json_object(eparam, '$._network_type'), '') as network_type,
        nvl(int(get_json_object(eparam, '$._net_status')), -1) as net_status,
        net,
        nvl(get_json_object(eparam, '$._event_timestamp'), '') as event_timestamp,
        from_unixtime(cast(substr(nvl(get_json_object(eparam, '$._event_timestamp'), '0'), 1, 10) as bigint)) as event_time,
        ts,
        tz,
        sts,
        wk_dt,
        bj_dt,
        IF(coalesce(lower(iso),'') in ('null','tran_unk',''),lower(get_json_object(public_params,'$.country_code')),lower(iso)) as iso,
        ctime,
        public_params,
        -- 事件属性字段
        nvl(int(get_json_object(eparam, '$._card_rank')), -1) as card_rank,
        nvl(get_json_object(eparam, '$._card_title'), '') as card_title,
        nvl(int(get_json_object(eparam, '$._click_result')), -1) as click_result,
        nvl(int(get_json_object(eparam, '$._click_type')), -1) as click_type,
        cast(nvl(get_json_object(eparam, '$._dur_time'), '0') as decimal(16, 4)) as dur_time,
        nvl(int(get_json_object(eparam, '$._element_type')), -1) as element_type,
        nvl(get_json_object(eparam, '$._enter_app'), '') as enter_app,
        nvl(get_json_object(eparam, '$._enter_type'), '') as enter_type,
        nvl(get_json_object(eparam, '$._fail_reason'), '') as fail_reason,
        nvl(get_json_object(eparam, '$._fail_result'), '') as fail_result,
        nvl(get_json_object(eparam, '$._from_type'), '') as from_type,
        nvl(int(get_json_object(eparam, '$._is_oriented')), -1) as is_oriented,
        nvl(get_json_object(eparam, '$._ocr_language'), '') as ocr_language,
        nvl(int(get_json_object(eparam, '$._ocr_nums')), -1) as ocr_nums,
        nvl(int(get_json_object(eparam, '$._ocr_result')), -1) as ocr_result,
        nvl(int(get_json_object(eparam, '$._on_off')), -1) as on_off,
        nvl(int(get_json_object(eparam, '$._oriented_type')), -1) as oriented_type,
        nvl(int(get_json_object(eparam, '$._page_type')), -1) as page_type,
        nvl(get_json_object(eparam, '$._params'), '') as params,
        nvl(get_json_object(eparam, '$._reason'), '') as reason,
        nvl(get_json_object(eparam, '$._scene_app'), '') as scene_app,
        nvl(int(get_json_object(eparam, '$._select_type')), -1) as select_type,
        nvl(get_json_object(eparam, '$._setting_info'), '') as setting_info,
        nvl(int(get_json_object(eparam, '$._setting_page_type')), -1) as setting_page_type,
        nvl(int(get_json_object(eparam, '$._setting_type')), -1) as setting_type,
        nvl(int(get_json_object(eparam, '$._stage')), -1) as stage,
        cast(nvl(get_json_object(eparam, '$._stay_time'), '0') as decimal(16, 4)) as stay_time,
        nvl(get_json_object(eparam, '$._target_version'), '') as target_version,
        nvl(get_json_object(eparam, '$._task_download_type'), '') as task_download_type,
        nvl(get_json_object(eparam, '$._task_id'), '') as task_id,
        nvl(get_json_object(eparam, '$._task_name'), '') as task_name,
        nvl(get_json_object(eparam, '$._task_product_name'), '') as task_product_name,
        nvl(get_json_object(eparam, '$._text_content'), '') as text_content,
        nvl(get_json_object(eparam, '$._toast_content'), '') as toast_content,
        nvl(int(get_json_object(eparam, '$._upgrade_from')), -1) as upgrade_from,
        nvl(int(get_json_object(eparam, '$._upgrade_type')), -1) as upgrade_type,
        nvl(get_json_object(eparam, '$._upload_status'), '') as upload_status,
        
        -- 动态分区，最近15天数据进行数据回溯
        if(datediff('${day_id}', substr(bj_dt, 0, 10)) <= 15 and datediff('${day_id}', substr(bj_dt, 0, 10)) >= 0,
            substr(bj_dt, 0, 10),
            '${day_id}'
        ) as p_date
    
    from tranai_dw.dwd_aiassistantvoice_2422_events_detail_di
    where p_date = regexp_replace('${day_id}', '-', '')
      and event in (
        'screenrecog_page_show',
        'screenrecog_page_click',
        'screenrecog_element_click',
        'screenrecog_upgrade_button_click',
        'screenrecog_app_enter',
        'screenrecog_app_exit',
        'screenrecog_element_show',
        'screenrecog_upload_status',
        'screenrecog_ocr',
        'screenrecog_upgrade_silent_on_off',
        'screenrecog_upgrade_page_show',
        'screenrecog_upgrade_fail',
        'screenrecog_button_click',
        'screenrecog_selected_text',
        'screenrecog_settings',
        'screenrecog_upgrade_newversion_get',
        'screenrecog_upgrade_download_begin',
        'screenrecog_upgrade_download_success',
        'screenrecog_upgrade_install_begin',
        'screenrecog_upgrade_install_success',
        'screenrecog_toast_show',
        'screenrecog_settings_button'
      )
) a0
left join (
    select device_id
    from tranai_dw.dwd_screenrecog_black_device_df
    where p_date = regexp_replace('${day_id}', '-', '')
    group by device_id
) a1 on a0.device_id = a1.device_id
where a1.device_id is null; 