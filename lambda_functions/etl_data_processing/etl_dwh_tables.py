import pandas as pd
from pangres import upsert


def etl_categories(connection):
    cmc_category_query = """select clean_date, 
    		a.tag as category,
    		sum(c.market_cap) as total_market_cap,
    		sum(
    		    case when c.ID in ('bitcoin', 'ethereum') then 0 
    		    else c.market_cap
    		    end
    		    ) as total_mcap_excl_btceth,
    		sum(c.volume) as total_volume,
    		sum(
    		    case when c.ID in ('bitcoin', 'ethereum') then 0 
    		    else c.volume
    		    end
    		    ) as total_volume_excl_btceth,
    		count(distinct a.ID) as total_project_count,
    		sum(
    		    case when c.ID in ('bitcoin', 'ethereum') then 0 
    		    else 1
    		    end
    		    ) as total_project_count_excl_btceth
        	from cmc_tags as a 
    	    left join cmc_main as b 
    		on a.ID = b.ID 
    	    inner join cg_hist_prices as c 
    		on b.slug = c.ID
    		where c.clean_date = current_date
    	    group by 1, 2"""

    cg_tags_query = """
    select clean_date, 
    	category,
    	sum(c.market_cap) as total_market_cap,
    	sum(
		    case when c.ID in ('bitcoin', 'ethereum') then 0 
		    else c.market_cap
		    end
		    ) as total_mcap_excl_btceth,
		sum(c.volume) as total_volume,
    	sum(
		    case when c.ID in ('bitcoin', 'ethereum') then 0 
		    else c.volume
		    end
		    ) as total_volume_excl_btceth,
    	count(distinct a.ID) as total_project_count,
    	sum(
		    case when c.ID in ('bitcoin', 'ethereum') then 0 
		    else 1
		    end
		    ) as total_project_count_excl_btceth
        from cg_categories as a 
        inner join cg_hist_prices as c 
    	on a.ID = c.ID
    	where c.clean_date = current_date
        group by 1, 2
    """

    market_rankings_query = """
    select 
    clean_date,
    ID,
    dense_rank() over (partition by clean_date order by market_cap desc) as market_cap_rank,
    dense_rank() over (partition by clean_date order by volume desc) as volume_rank,
    cast(volume/market_cap as decimal(20,10)) as turnover_ratio
    from cg_hist_prices
    where clean_date = current_date
    """

    df_cg = pd.read_sql_query(cg_tags_query, connection)
    df_cmc = pd.read_sql_query(cmc_category_query, connection)
    df_market = pd.read_sql_query(market_rankings_query, connection)

    df_cg = df_cg.set_index(['clean_date', 'category'])
    cf_cmc = df_cmc.set_index(['clean_date', 'category'])

    with connection.connect() as cur:
        upsert(con=cur,
               df=df_cg,
               table_name='cg_categories_market_data',
               if_row_exists='update')

        upsert(con=cur,
               df=cf_cmc,
               table_name='cmc_tag_market_data',
               if_row_exists='update')

    df_market.to_sql('ranking_metrics', connection, if_exists='append', index=False)

    return
