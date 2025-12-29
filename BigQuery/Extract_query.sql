WITH manual_save_data AS (
    SELECT
        project_id,
        data_id,
        object_count,
        utcnow # 이벤트 기록 시간
    FROM
        crowdworks-platform.crowdworks_dm.cw_object_event_data
    WHERE 1=1
        AND (project_start_date >= "2025-09-04 00:00:00" AND project_start_date < "2025-10-14 23:59:59")
        AND project_id IN ("26628")
        AND action_commit = 'manual_save' # 수동저장 이벤트 기록 추출
    QUALIFY ROW_NUMBER() OVER(PARTITION BY data_id ORDER BY utcnow DESC) = 1 # 각 data_id별로 가장 최근에 저장된 이벤트 기록 딱 하나
)
SELECT 
    TPD.project_id,
    TPD.data_idx,
    TPD.src_idx,
    TPD.prog_state_cd,
    TPD.problem_yn,
    TPD.problem_reason,
    TPD.work_object_number,
    IF(OL.data_id IS NOT NULL, 'Y', 'N') as is_modified,
    IF(OL.data_id IS NOT NULL, OL.object_count, TPD.work_object_number) as final_object_count,
    WP.member_id as worker_id,
    WP.nickname as worker_nickname,
    CP.member_id as checker_id,
    CP.nickname as checker_nickname,    
    TPD.work_edate,
    TPD.check_edate,
    DATETIME(TIMESTAMP(OL.utcnow), 'Asia/Seoul') as modified_dt,
    CONCAT('https://client.crowdworks.kr/project/monitor/',TPD.project_id,'/?dataId=',TPD.data_idx) as link
FROM
    crowdworks-platform.crowdworks_dw.tb_prj_data TPD
LEFT OUTER JOIN
    crowdworks-platform.crowdworks_dw.works_user_profile WP
ON
    TPD.work_user = WP.member_id
LEFT OUTER JOIN
    crowdworks-platform.crowdworks_dw.works_user_profile CP
ON
    TPD.check_user = CP.member_id
LEFT OUTER JOIN
    crowdworks-platform.crowdworks_dw.works_project PRJ
ON
    TPD.project_id = PRJ.project_id
LEFT OUTER JOIN
    manual_save_data OL
ON
    TPD.data_idx = CAST(OL.data_id AS INT64)
WHERE 1=1
    AND (TPD.project_start_date >= "2025-09-04 00:00:00" AND TPD.project_start_date < "2025-10-14 23:59:59")
    AND TPD.project_id IN (26628)
    AND TPD.check_edate BETWEEN "2025-09-04 00:00:00" AND "2025-10-14 23:59:59"
    AND TPD.prog_state_cd IN ("CHECK_END","ALL_FINISHED")
    AND TPD.is_deleted IS NOT TRUE
ORDER BY
  TPD.data_idx
