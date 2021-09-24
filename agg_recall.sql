create or replace table app_risk.app_risk_test.rule_recall as
SELECT
cb.payment_token
,cb.user_token
,cb.currency_code
,to_date(cb.payment_created_at) payment_dt
--,rule_name
,case when rule_category = 'backstop' then 1 else 0 end as backstop_flag
,case when rule_category = 'heuristic' then 1 else 0 end as heuristic_flag
,lt.business_category as MCC
,datediff(mon, lt.first_card_payment_date,cb.payment_created_at) as seller_tenure_month
,case when seller_tenure_month < 6 or seller_tenure_month is null then '0.<6mon'
when seller_tenure_month < 12 then '1.6mon - 12mon'
when seller_tenure_month < 24 then '2.12mon - 24mon'
when seller_tenure_month>= 24 then '3.over 24mon'
else 'other' end as tenure_band
,summ_GPV_trailing_quarter as gpv_t90d
,summ_GPV_trailing_quarter*4 as annualized_gpv
,case when annualized_gpv < 250000 then '0.100k - 250k'
when annualized_gpv < 1000000 then '1.250k - 1M'
when annualized_gpv >= 1000000 then '2.over 1M'
else 'other' end as annual_gpv_band
,CASE WHEN LOWER(cb.entry_method) in
('register_contactless', 'register_emv_contact', 'register_swiped', 'register_hardware_emv_contact', 'register_hardware_contactless', 'register_hardware_swiped') THEN 'CP'
WHEN LOWER(cb.entry_method) in ('register_manually_keyed', 'register_card_on_file', 'register_hardware_manually_keyed', 'register_hardware_card_on_file') THEN 'CNP'
WHEN LOWER(cb.entry_method) in ('terminal_keyed', 'terminal_card_on_file') THEN 'VT'
WHEN LOWER(cb.entry_method) in ('invoice_on_file', 'invoice_web_form') THEN 'Invoice'
WHEN LOWER(cb.entry_method) in ('external_api_on_file', 'external_api') THEN 'API'
ELSE 'Others' END AS entry_method
,case when cr.user_token is not null then 1 else 0 end as model_flag
,cb.payment_cents/100 as payment_dllr
,cb.chargeback_cents/100 as chargeback_dllr
,case when pmt_row_num = 1 or pmt_row_num is null then chargeback_dllr else 0 end as chargeback_dllr_dedup
,chargeback_dllr * model_flag as chargeback_dllr_caught
,case when pmt_row_num = 1 or pmt_row_num is null then chargeback_dllr_caught else 0 end as chargeback_dllr_caught_dedup
,cb.loss_cents/100 as loss_dllr
,case when pmt_row_num = 1 or pmt_row_num is null then loss_dllr else 0 end as loss_dllr_dedup
,cb.loss_cents/100 * model_flag as loss_dllr_caught
,case when pmt_row_num = 1 or pmt_row_num is null then loss_dllr_caught else 0 end as loss_dllr_caught_dedup
,case when model_flag = 1 then cb.user_token else null end as user_token_caught
,case when backstop_flag = 1 then chargeback_dllr_caught else 0 end as backstop_caught_cb_dllr
,case when heuristic_flag = 1 then chargeback_dllr_caught else 0 end as heuristic_caught_cb_dllr
,case when backstop_flag = 1 then loss_dllr_caught else 0 end as backstop_caught_loss_dllr
,case when heuristic_flag = 1 then loss_dllr_caught else 0 end as heuristic_caught_loss_dllr
,case when backstop_flag = 1 then cb.user_token else null end as backstop_flag_token
,case when heuristic_flag = 1 then cb.user_token else null end as heuristic_flag_token
from APP_RISK.APP_RISK.CHARGEBACKS cb
LEFT OUTER JOIN APP_BI.PENTAGON.aggregate_seller_lifetime_summary lt ON cb.user_token = lt.unit_token
left join (select -- calculate quarterly trailing gpv
distinct cb.user_token, to_date(cb.payment_created_at) as payment_dt,
sum(case when datediff(days,sds.PAYMENT_TRX_RECOGNIZED_DATE,payment_dt) <= 90 and payment_dt > sds.PAYMENT_TRX_RECOGNIZED_DATE
then GPV_PAYMENT_AMOUNT_BASE_UNIT_USD/100 else 0 end) as summ_GPV_trailing_quarter
from APP_RISK.APP_RISK.CHARGEBACKS cb
left join app_bi.pentagon.aggregate_seller_daily_payment_summary sds on cb.user_token = sds.unit_token
group by 1,2) gpv
on gpv.user_token = cb.user_token and gpv.payment_dt = to_date(cb.payment_created_at)
left join (select -- find model flagged payments
distinct cb.user_token
,payment_token
,to_date(cb.payment_created_at) as payment_dt
--, cr.rule_name
,rule_category
,row_number() over (partition by payment_token,payment_dt order by rule_category) as pmt_row_num -- count for multiple flagging rules for same payment_token
,MAX(case when cr.user_token is not null THEN 1 else 0 END) AS model_flag
from app_risk.app_risk.chargebacks cb
left join creditactions.RAW_OLTP.CREDIT_RISK_RULE_RESULTS AS cr
ON cr.user_token = cb.user_token
AND TO_TIMESTAMP(cr.created_at_millis/1000) < DATEADD(DAY,1,to_date(cb.payment_created_at))
AND TO_TIMESTAMP(cr.created_at_millis/1000) >= DATEADD(DAY,-91,to_date(cb.payment_created_at))
left join app_risk.app_risk_test.rule_category on cr.rule_name = rule_category.rule_name
/* where cr.rule_name in (select distinct model_name as rule_name -- only inlcude active rules
from riskarbiter.raw_oltp.rule_configs
where model_name in (select distinct model_name
from riskarbiter.raw_oltp.rule_configs
where HOOK_DEF_TOKEN in ('HD_67' , 'HD_5')  -- HD 67: CREDIT RISK SSP, HD 5: CREDIT RISK
and active = TRUE
and to_timestamp(effective_at/1000) < current_date() -- can add time frame as well, i.e. >= xxxx-xx-xx and < xxxx-xx-xx
and create_suspicions = TRUE)
and active = true) */
GROUP BY 1,2,3,4) cr
on cr.payment_token = cb.payment_token
where cb.reason_code_type = 'credit' and cb.payment_created_at >= '2020-07-01' and cb.CURRENCY_CODE = 'USD' and chargeback_dllr>100 and gpv_t90d > 25000
;
