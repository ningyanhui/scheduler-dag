set mapreduce.job.name=dws_screenrecog_user_behavior_di_etl;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table tranai_dw.dws_screenrecog_user_behavior_di partition(p_date, insert_date)
select
    device_id,
    gaid,
    country_code,
    device_model,
    device_brand,
    app_version_fmt,
    app_package_name,
    
    -- 聚合计算
    count(distinct event_name) as event_type_count,
    count(*) as event_total_count,
    
    -- 计算使用时长
    sum(dur_time) as total_duration,
    
    -- 计算首次活跃时间
    min(event_timestamp) as first_active_time,
    
    -- 计算最近一次活跃时间
    max(event_timestamp) as last_active_time,
    
    -- 计算页面停留时长
    sum(case when event_name = 'screenrecog_page_show' then stay_time else 0 end) as page_stay_duration,
    
    -- 计算OCR识别次数
    sum(case when event_name = 'screenrecog_ocr' then 1 else 0 end) as ocr_count,
    
    -- 计算OCR成功率
    sum(case when event_name = 'screenrecog_ocr' and ocr_result = 1 then 1 else 0 end) / 
    GREATEST(sum(case when event_name = 'screenrecog_ocr' then 1 else 0 end), 1) as ocr_success_rate,
    
    -- 计算页面跳转次数
    sum(case when event_name = 'screenrecog_page_click' then 1 else 0 end) as page_navigation_count,
    
    -- 计算应用进入退出次数
    sum(case when event_name = 'screenrecog_app_enter' then 1 else 0 end) as app_enter_count,
    sum(case when event_name = 'screenrecog_app_exit' then 1 else 0 end) as app_exit_count,
    
    -- 计算升级相关行为
    sum(case when event_name like 'screenrecog_upgrade%' then 1 else 0 end) as upgrade_related_count,
    sum(case when event_name = 'screenrecog_upgrade_install_success' then 1 else 0 end) as upgrade_success_count,
    
    -- 计算选中文本相关行为
    sum(case when event_name = 'screenrecog_selected_text' then 1 else 0 end) as text_selection_count,
    
    -- ETL处理时间
    current_timestamp() as etl_time,
    
    -- 动态分区
    p_date,
    '${day_id}' as insert_date
    
from tranai_dw.dwd_screenrecog_event_detail_di
where p_date = '${day_id}'
  and device_id is not null
  and device_id != ''
group by 
    device_id,
    gaid,
    country_code,
    device_model,
    device_brand,
    app_version_fmt,
    app_package_name,
    p_date
; 