/******************************************************************************
Author    : Juliana Diaz
Created   : 2022-06-10
Reason    : Commit for the View I'm Showing
*******************************************************************************/
CREATE OR REPLACE VIEW ${ENVIRONMET}_INVESTMENT_INTEL.PUBLIC.VW_RPT_THE_VIEW_IM_SHOWING AS
(   
SELECT
    -- Key Fields
        T2.ASSET_KEY,
        T2.DATE_KEY,
        T2.PORTFOLIO_KEY,
        T2.FULL_DATE,
        
    -- Measures and Attributes
        T2.CUSIP,
        T2.SOURCE,
        T2.ALADDIN_ID,
        T2.SOURCE_REPORTING_DATE,
        T2.MATURITY,
        T2.DATE_DIFF,
        T2.MATURITY_BUCKETS,
        T2.MATURITY_BUCKETS_SORT,
        T2.EFFECTIVE_RATING,
    /**REMOVING FOR SECURITY REASONS**/
        T2.SECURITY_GROUP,
        T2.YIELD_TO_MATURITY,
        T2.SECTOR_PATH_LMG_CORE,
        T2.SECURITY_TYPE,
        T2.PORTFOLIO_CODE,
        T2.PORTFOLIO_NAME,
        T2.FIPOP_TREE_LEVEL_1,
        T2.FIPOP_TREE_LEVEL_2,
        T2.FIPOP_TREE_LEVEL_3,
        T2.FIPOP_TREE_LEVEL_4,
        T2.TEAM_TREE_LEVEL_2,
        T2.TEAM_TREE_LEVEL_3,
        T2.TEAM_TREE_LEVEL_4,
        T2.TEAM_TREE_LEVEL_5
    FROM (
        WITH latest_reporting_date (LATEST_DATE) AS
            (SELECT
            MAX(SOURCE_REPORTING_DATE) AS LATEST_REPORTING_DATE
            FROM ${ENVKEY}_PORTFOLIO_INTEL.PUBLIC.FACT_DAILY_IBOR_POSITIONS T1
            INNER JOIN (
                SELECT  
                    DISTINCT 
                    PORTFOLIO_ID,
                    PORTFOLIO_CODE,
                    PORTFOLIO_NAME,
                    PORTFOLIO_GROUP_NAME
                FROM
                     ${ENVKEY}_PORTFOLIO_INTEL.PUBLIC.VW_DIM_PORTFOLIO_CURRENT
                WHERE 1=1
                    AND (
                        TEAM_TREE_LEVEL_5 = 'CORE-USD'
                        OR (
                                FIPOP_TREE_LEVEL_2 IN ('A LIST OF PORTFOLIO CODES')
                            OR FIPOP_TREE_LEVEL_3 IN ('A LIST OF PORTFOLIO CODES')
                            OR FIPOP_TREE_LEVEL_4 IN ('A LIST OF PORTFOLIO CODES')
                            )
                            OR (
                                FIPOP_TREE_LEVEL_5 = 'A LIST OF PORTFOLIO CODES'
                                )
                        )
                )P 
                    ON  T1.PORTFOLIO_ID = P.PORTFOLIO_ID)
        SELECT 
            T4.CUSIP,
            D.FULL_DATE,
            T4.ASSET_KEY,
            T1.PORTFOLIO_KEY,
            T1.DATE_KEY,
            T4.SOME_ID,
            T1.SOURCE_REPORTING_DATE,
            TRY_TO_DATE(LEFT(T4.MATURITY_DATE,10))                                   AS MATURITY,
            YEAR(TRY_TO_DATE(LEFT(T4.MATURITY_DATE,10)))-D.YEAR_KEY  AS DATE_DIFF,
            CASE
                WHEN T4.SECURITY_TYPE ILIKE '%STIF%' THEN '0 - 1'
                WHEN YEAR(TRY_TO_DATE(LEFT(T4.MATURITY_DATE,10)))-D.YEAR_KEY  < 1 THEN '0 - 1'
                WHEN YEAR(TRY_TO_DATE(LEFT(T4.MATURITY_DATE,10)))-D.YEAR_KEY  >= 1 AND YEAR(TRY_TO_DATE(LEFT(T4.MATURITY_DATE,10)))-D.YEAR_KEY  < 5 THEN '1 - 5'
                WHEN YEAR(TRY_TO_DATE(LEFT(T4.MATURITY_DATE,10)))-D.YEAR_KEY  >= 5 AND YEAR(TRY_TO_DATE(LEFT(T4.MATURITY_DATE,10)))-D.YEAR_KEY  < 10 THEN '5 - 10'
                WHEN YEAR(TRY_TO_DATE(LEFT(T4.MATURITY_DATE,10)))-D.YEAR_KEY  >= 10 AND YEAR(TRY_TO_DATE(LEFT(T4.MATURITY_DATE,10)))-D.YEAR_KEY  < 20 THEN '10 - 20'
                WHEN YEAR(TRY_TO_DATE(LEFT(T4.MATURITY_DATE,10)))-D.YEAR_KEY  >= 20 THEN '20 +'
            ELSE 'N/A'
                END                                                             AS MATURITY_BUCKETS,
            CASE
                WHEN T4.SECURITY_TYPE ILIKE '%STIF%' THEN 1
                WHEN YEAR(TRY_TO_DATE(LEFT(T4.MATURITY_DATE,10)))-D.YEAR_KEY  < 1 THEN 1
                WHEN YEAR(TRY_TO_DATE(LEFT(T4.MATURITY_DATE,10)))-D.YEAR_KEY  >= 1 AND YEAR(TRY_TO_DATE(LEFT(T4.MATURITY_DATE,10)))-D.YEAR_KEY  < 5 THEN 2
                WHEN YEAR(TRY_TO_DATE(LEFT(T4.MATURITY_DATE,10)))-D.YEAR_KEY  >= 5 AND YEAR(TRY_TO_DATE(LEFT(T4.MATURITY_DATE,10)))-D.YEAR_KEY  < 10 THEN 3
                WHEN YEAR(TRY_TO_DATE(LEFT(T4.MATURITY_DATE,10)))-D.YEAR_KEY  >= 10 AND YEAR(TRY_TO_DATE(LEFT(T4.MATURITY_DATE,10)))-D.YEAR_KEY  < 20 THEN 4
                WHEN YEAR(TRY_TO_DATE(LEFT(T4.MATURITY_DATE,10)))-D.YEAR_KEY  >= 20 THEN 5 
            ELSE 6
                END                                                             AS MATURITY_BUCKETS_SORT,
                T4.NOTCHED_EFFECTIVE_RATING                                     AS EFFECTIVE_RATING,
            CASE WHEN  T4.NOTCHED_EFFECTIVE_RATING='NA' THEN 'NA'
            ELSE T4.UNNOTCHED_EFFECTIVE_RATING END                              AS EFFECTIVE_RATING_GROUP,
            CASE
                WHEN T4.UNNOTCHED_EFFECTIVE_RATING = 'AAA' THEN 1
                WHEN T4.UNNOTCHED_EFFECTIVE_RATING = 'AA' THEN 2
                WHEN T4.UNNOTCHED_EFFECTIVE_RATING = 'A' THEN 3
                WHEN T4.UNNOTCHED_EFFECTIVE_RATING = 'BBB' THEN 4
                /**REMOVING FOR SECURITY REASONS**/
                WHEN T4.UNNOTCHED_EFFECTIVE_RATING = 'DDD' THEN 10
                WHEN T4.UNNOTCHED_EFFECTIVE_RATING = 'DD' THEN 11
                WHEN T4.UNNOTCHED_EFFECTIVE_RATING = 'D' THEN 12
            ELSE 13
            END                                                                 AS EFFECTIVE_RATING_GROUP_SRT,
            CASE
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'AAA+' THEN 1
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'AAA' THEN 2
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'AAA-' THEN 3
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'AA+' THEN 4
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'AA' THEN 5
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'AA-' THEN 6
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'A+' THEN 7
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'A' THEN 8
                /**REMOVING FOR SECURITY REASONS**/
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'C' THEN 26
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'C-' THEN 27
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'DDD+' THEN 28
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'DDD' THEN 29
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'DDD-' THEN 30
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'DD+' THEN 31
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'DD' THEN 32
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'DD-' THEN 33
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'D+' THEN 34
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'D' THEN 35
                WHEN T4.NOTCHED_EFFECTIVE_RATING = 'D-' THEN 36
            ELSE 37
            END                                                     AS EFFECT_RATING_SORT,
            T1.MARKET_VALUE_USD,
            T1.SOURCE,
            P.PORTFOLIO_GROUP_NAME,
            T4.SECURITY_GROUP,
            T4.SECURITY_TYPE,
         /**REMOVING FOR SECURITY REASONS**/
            P.FIPOP_TREE_LEVEL_2,
            P.FIPOP_TREE_LEVEL_3,
            P.FIPOP_TREE_LEVEL_4,
            P.TEAM_TREE_LEVEL_2,
            P.TEAM_TREE_LEVEL_3,
            P.TEAM_TREE_LEVEL_4,
            P.TEAM_TREE_LEVEL_5
            FROM ${ENVKEY}_SOME_DATABASE.PUBLIC.SOME_TABLE T1
            LEFT JOIN ${ENVKEY}_SOME_DATABASE.PUBLIC.SOME_DATE_TABLE AS D ON T1.DATE_KEY=D.DATE_KEY
                LEFT JOIN ${ENVKEY}_SOME_DATABASE.PUBLIC.SOME_TABLE T4
                    ON T1.ASSET_KEY=T4.ASSET_KEY
            INNER JOIN (
                SELECT  
                    DISTINCT 
                    PORTFOLIO_ID,
                    PORTFOLIO_CODE,
                    PORTFOLIO_NAME,
                    PORTFOLIO_GROUP_NAME,
                    /**REMOVING FOR SECURITY REASONS**/
                    TEAM_TREE_LEVEL_2,
                    TEAM_TREE_LEVEL_3,
                    TEAM_TREE_LEVEL_4,
                    TEAM_TREE_LEVEL_5
                FROM
                     ${ENVKEY}_PORTFOLIO_INTEL.PUBLIC.VW_DIM_PORTFOLIO_CURRENT
                WHERE 1=1
                          AND (
                                      TEAM_TREE_LEVEL_5 = 'CORE-USD'
                                     OR (
                                            FIPOP_TREE_LEVEL_2 IN ('A LIST OF PORTFOLIO CODES')
                                         OR FIPOP_TREE_LEVEL_3 IN ('A LIST OF PORTFOLIO CODES')
                                         OR FIPOP_TREE_LEVEL_4 IN ('A LIST OF PORTFOLIO CODES')
                                     )
                                     OR 
                                         FIPOP_TREE_LEVEL_5 = 'SOMETHING'
                                    
                               )
                        AND PORTFOLIO_CODE NOT IN ('A LIST OF PORTFOLIO CODES')
                ) P
                    ON  T1.PORTFOLIO_ID = P.PORTFOLIO_ID, latest_reporting_date  
                WHERE T1.SOURCE_REPORTING_DATE= latest_reporting_date.LATEST_DATE
                AND LENGTH(TRIM(NOTCHED_EFFECTIVE_RATING)) > 0
        ) T2)