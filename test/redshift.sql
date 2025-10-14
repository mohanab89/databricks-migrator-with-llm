/****************************************************************************************************************************
****************************************************************************************************************************/
DROP TABLE IF EXISTS mpp;
CREATE temp TABLE mpp AS (
select
    cl.aw_account_id as product_account_id
     , gpp.customer_id
     , gpp.primary_product_12m
     , gpp.primary_product_30d
from
            (select customer_id, primary_product||' '||strength as primary_product_12m, primary_product_30d ||' '||strength_30d as primary_product_30d
            from analyst_prod.groupwide_primary_product
            where report_date = (select max(report_date) from analyst_prod.groupwide_primary_product)) gpp
            left join
            fdg.dim_fdg_customer_lifecycle cl
            on gpp.customer_id = cl.customer_id
            and cl.dw_date = (select max (dw_date) from fdg.dim_fdg_customer_lifecycle)
            -- exclude racing only customer_ids who do not have an a&w product account id
            where aw_account_id is not null
) ;
/****************************************************************************************************************************
****************************************************************************************************************************/
DROP TABLE IF EXISTS gameplay;
CREATE temp TABLE gameplay AS (
SELECT
    gp.product_account_id,
    date_trunc('day',gp.start_date) as start_date,
    gp.brand,
    case when gp.bet_location = 'mr' then 'ct' else gp.bet_location end as bet_location ,
    gp.channel,
    sum(gp.stake) as stake,
    sum(gp.win) as win,
    sum(gp.ggr)  as ggr,
    sum(gp.bonus_cost) as bonus_cost,
    sum(gp.ngr) as ngr,
    SUM(CASE WHEN RTP IS NULL THEN stake * (4.5/100)
        ELSE (stake*(100-RTP)/100) END) as eggr,
    SUM(CASE WHEN RTP IS NULL THEN stake * (4.5/100)
        ELSE (stake*(100-RTP)/100) END) - sum(bonus_cost) as eNGR
from fdg.fact_casino_gameplay_aw gp
left join fdg.dim_aw_account aw
    on aw.product_account_id = gp.product_account_id
left JOIN analyst_dev.casino_commercial_dim_game cg
    ON gp.game_type_id=cg.game_type_id
where gp.product_account_id > 0 and--remove unused accounts
      aw.test_account = 'false' and --remove test accounts
      aw.is_admin = 'false' and
      aw.admin_account = 'false' and
      gp.start_date::date >= DATEADD(month, -13, date_trunc('month',getdate()))::date --change to 13 if running into extract issues
      and gp.start_date::date < getdate()::date
group by 1,2,3,4,5
);
/****************************************************************************************************************************
****************************************************************************************************************************/
DROP TABLE IF EXISTS fvm;
CREATE temp TABLE fvm AS (
select
    product_account_id
    ,fvm_month
    ,fvm_value_tier
    ,fvm_intra_tier --added 4/29/25
from
    (select product_account_id
            ,date_trunc('month',week_date) as fvm_month
            ,week_date
            ,fvm_value_tier
            ,fvm_intra_tier --added 4/29/25
            ,ROW_NUMBER() OVER (PARTITION BY product_account_id, date_trunc('month',week_date) ORDER BY week_date desc) as rnk
          from analyst_prod.casino_fvm_segment
          where week_date > '2023-07-21'
          )fvm
        where fvm.rnk = 1
);
/****************************************************************************************************************************
****************************************************************************************************************************/
DROP TABLE IF EXISTS clc;
CREATE temp TABLE clc AS (
select
    lc.product_account_id
    ,lc.cross_sell_flag
    ,lc.first_stake_real_money_date
    ,gw.group_activation_date
from fdg.dim_casino_account_lifecycle lc
left join fdg.dim_aw_groupwide_lifecycle gw
    on gw.product_account_id = lc.product_account_id
where lc.first_stake_date is not null
    and gw.cas_first_stake_date is not null
)
;
/****************************************************************************************************************************
****************************************************************************************************************************/
DROP TABLE IF EXISTS analyst_cas.cas_comm_tde;
CREATE TABLE analyst_cas.cas_comm_tde AS (
SELECT
    gp.product_account_id,
    gp.start_date,
    lc.group_activation_date,
    gp.brand,
    gp.bet_location,
    gp.channel,
    seg.last_month_tier,
    mpp.primary_product_30d as product_segment,
    case when bet_location in ('ct','mr') then 'xSell' else lc.cross_sell_flag end as cross_sell_flag,
    null as lifetime_casino_app_pref,
    fvm.fvm_value_tier,
    fvm.fvm_intra_tier, --added 4/29/25
    gp.stake,
    gp.win,
    gp.ggr,
    gp.bonus_cost,
    gp.ngr,
    gp.eggr,
    gp.eNGR
from gameplay gp
left join mpp
    on mpp.product_account_id = gp.product_account_id
left join clc lc
    on lc.product_account_id = gp.product_account_id
left join fdg.dim_casino_customer_segment seg
    on gp.product_account_id = seg.product_account_id
    and dateadd('month',1,seg.date::date)::date = date_trunc('month',gp.start_date::date)::date
left join fvm
    on fvm.product_account_id = gp.product_account_id
    and dateadd('month',1,fvm_month) = date_trunc('month',gp.start_date)::date
);
/****************************************************************************************************************************
****************************************************************************************************************************/


-- /Volumes/users/mohana_basak/mohana_test_vol/converter_input/redshift_mehran/
