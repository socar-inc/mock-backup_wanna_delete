WITH _tmp AS (
SELECT *
FROM `socar-data-dev.bbiyak_test.car_zone_day`
),
__tmp AS (
SELECT *
FROM _tmp as czd
LEFT JOIN `socar-data-dev.bbiyak_test.finance_raw` AS fr
ON czd.zone_id = fr.zone_id
),
___tmp AS (
SELECT *
FROM `socar-data-dev.bbiyak_test.reservation_biz_detail` as rbd
)
SELECT *
FROM ___tmp
