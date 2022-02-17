create or replace table app_risk.app_risk_test.rule_precision as
SELECT 
    gpv.user_token    
    ,gpv.rule_name
    ,rule_category
    ,gt_25k_flag
    ,payment_trx_recognized_date as date
        ,CASE WHEN gpv.flagged=1 THEN gpv.user_token END AS rule_cased_seller
        ,CASE WHEN gpv.flagged=1 AND cb.user_token is not NULL THEN gpv.user_token END AS rule_cased_cb_seller
        ,CASE WHEN gpv.flagged=1 AND cb.loss_cents>0 THEN gpv.user_token END AS rule_cased_loss_seller
        , gpv.gpv_payment_amount_base_unit/100 AS gpv_dllr
        , gpv.gpv_payment_amount_base_unit/100*gpv.flagged AS gpv_dllr_caught
        , CASE WHEN cb.user_token is not NULL AND gpv.flagged=1 THEN gpv_payment_amount_base_unit/100 ELSE 0 END AS gpv_dllr_caught_chargeback
        , CASE WHEN cb.loss_cents >0 AND gpv.flagged=1 THEN gpv_payment_amount_base_unit/100 ELSE 0 END AS gpv_dllr_caught_loss
        ,cb.chargeback_cents/100 AS chargeback_dllr
        ,cb.chargeback_cents/100*gpv.flagged AS chargeback_caught_dllr
         FROM (SELECT sds.user_token
        ,sds.payment_trx_recognized_date
        ,sds.gpv_payment_amount_base_unit
        ,cr.rule_name
        ,rule_category
        ,case when sum(sds.gpv_payment_amount_base_unit)*4 > 2500000 then 1
               else 0 end as gt_25k_flag
        ,MAX(CASE WHEN cr.user_token is NOT NULL THEN 1 ELSE 0 END) AS flagged
        FROM app_bi.pentagon.aggregate_seller_daily_payment_summary AS sds
        LEFT OUTER JOIN creditactions.RAW_OLTP.CREDIT_RISK_RULE_RESULTS AS cr
         ON cr.user_token = sds.user_token 
         AND TO_TIMESTAMP(cr.created_at_millis/1000) < DATEADD(DAY,1,sds.payment_trx_recognized_date)
         AND TO_TIMESTAMP(cr.created_at_millis/1000) >= DATEADD(DAY,-91,sds.payment_trx_recognized_date)
        left join app_risk.app_risk_test.rule_category on cr.rule_name = rule_category.rule_name
        left join app_risk.app_risk.okr2021h1_credit_ops_actions AS cr_action ON sds.user_token = cr_action.user_token AND TO_DATE(cr_action.CREATED_AT) < DATEADD(DAY,1,sds.payment_trx_recognized_date) AND TO_DATE(cr_action.CREATED_AT) >= DATEADD(DAY,-91,sds.payment_trx_recognized_date)
         WHERE sds.currency_code = 'USD' AND sds.payment_trx_recognized_date >= '2020-01-01'
         GROUP BY 1,2,3,4,5) gpv
         LEFT OUTER JOIN 
         APP_RISK.APP_RISK.chargebacks AS cb  ON gpv.user_token = cb.user_token AND to_date(cb.payment_created_at) = gpv.payment_trx_recognized_date
        AND cb.type = 'credit' AND cb.chargeback_cents > 10000
        --AND cb.status = 'LOST' AND cb.is_protected = 0 
        WHERE rule_name is not null
;
