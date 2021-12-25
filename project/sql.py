script_create_table_prices = """DROP TABLE IF EXISTS prices;
                                CREATE TABLE IF NOT EXISTS prices (date DATE NOT NULL,
                                                                    model INTEGER NOT NULL,
                                                                    retailer TEXT NOT NULL,
                                                                    kind_cheese TEXT NOT NULL,
                                                                    key TEXT NOT NULL,
                                                                    price INTEGER NOT NULL);"""

script_create_table_customers = """DROP TABLE IF EXISTS customers;
                                    CREATE TABLE IF NOT EXISTS customers (customer_id INTEGER PRIMARY KEY,
                                                                        family_type TEXT NOT NULL,
                                                                        number_people INTEGER NOT NULL,
                                                                        financial_wealth TEXT NOT NULL,
                                                
                                                                        favorite_kind_cheese TEXT NOT NULL,
                                                                        avg_cheese_consumption INTEGER NOT NULL,
                                                                        stop_eating_cheese TEXT NOT NULL,
                                                                        switching_another_cheese TEXT NOT NULL,
                                                
                                                                        basic_retailer TEXT NOT NULL,
                                                                        significant_price_change INTEGER NOT NULL,
                                                                        looking_cheese TEXT NOT NULL,
                                                                        sensitivity_marketing_campaign TEXT NOT NULL,
                                                                        key TEXT NOT NULL);"""

script_create_table_stocks = """DROP TABLE IF EXISTS stocks;
                                CREATE TABLE IF NOT EXISTS stocks (customer_id INTEGER PRIMARY KEY,
                                                                    current_value INTEGER NOT NULL);"""

script_create_table_sales = """DROP TABLE IF EXISTS sales;
                                CREATE TABLE IF NOT EXISTS sales (date DATE NOT NULL,
                                                                  model INTEGER NOT NULL,                                    
                                                                  retailer TEXT NOT NULL,
                                                                  customer_id INTEGER NOT NULL,
                                                                  kind_cheese TEXT NOT NULL,
                                                                  quantity INTEGER NOT NULL,
                                                                  price INTEGER NOT NULL);"""

script_read_table_sales = """SELECT * FROM sales as s WHERE s.date > '{dt}';"""

script_add_table_sales_start_records = """INSERT INTO sales (date, model, retailer, customer_id, 
                                                                kind_cheese, quantity, price)
                                        SELECT pr.date, pr.model, pr.retailer, c.customer_id, 
                                                pr.kind_cheese, s.current_value, pr.price
                                        FROM customers as c LEFT JOIN prices as pr ON c.key = pr.key
					                                        LEFT JOIN stocks as s ON c.customer_id = s.customer_id
                                        WHERE pr.date = '{dt}' AND pr.model = {model}
                                        ORDER BY c.customer_id;"""

script_add_table_sales_record = """INSERT INTO sales (date, model, retailer, customer_id, kind_cheese, quantity, price)
                                    VALUES ('{dt}', 
                                            {model}, 
                                            '{retailer}', 
                                            {customer_id}, 
                                            '{kind_cheese}', 
                                            {quantity}, 
                                            {price})"""

script_get_customer_data = """SELECT c.customer_id,
                               c.family_type,
                               c.number_people,
                               c.financial_wealth,
                               c.favorite_kind_cheese,
                               c.avg_cheese_consumption,
                               c.stop_eating_cheese,
                               c.switching_another_cheese,
                               c.basic_retailer,
                               c.significant_price_change,
                               c.looking_cheese,
                               c.sensitivity_marketing_campaign,
                               c.key,
                               s.current_value
                        FROM public.customers as c LEFT JOIN public.stocks as s ON c.customer_id = s.customer_id
                        WHERE c.customer_id = {customer_id}"""

script_get_customer_last_price = """SELECT s2.price
                                    FROM sales as s2
                                    WHERE s2.customer_id = {customer_id}
                                            AND s2.model = {model} 
                                            AND s2.date = (SELECT max(s1.date)
				                                           FROM sales as s1
				                                           WHERE s1.customer_id = {customer_id}
				                                                  AND s1.model = {model});"""

script_get_retailer_current_price = """SELECT pr.price
                                       FROM prices as pr
                                       WHERE pr.date = '{dt}'
                                            AND pr.model = {model}
                                            AND pr.retailer = '{retailer}' 
                                            AND pr.kind_cheese = '{kind_cheese}';"""

script_get_retailer_cheaper_kind_cheese = """WITH t as (SELECT pr.kind_cheese, pr.price
                                                        FROM prices as pr
                                                        WHERE pr.date = '{dt}' 
                                                              AND pr.model = {model} 
                                                              AND pr.retailer='{retailer}'
                                                              AND pr.price < {retailer_current_price}
                                                        LIMIT 1)

                                            SELECT pr.kind_cheese, pr.price
                                            FROM prices as pr
                                            WHERE pr.date = '{dt}' 
                                                  AND pr.model = {model} 
                                                  AND pr.retailer='{retailer}'
                                                  AND pr.price <= {retailer_current_price}
                                            LIMIT 1
                                            OFFSET (SELECT count(*) FROM t)
                                                                    """
script_get_retailer_min_price = """SELECT pr.retailer,pr.price
                                   FROM prices as pr
                                   WHERE pr.date = '{dt}' 
                                        AND pr.model = {model} 
                                        AND pr.retailer in ({retailer_top_list})
                                        AND pr.kind_cheese = '{current_kind_cheese}'
                                    ORDER BY pr.price
                                    LIMIT 1
                                    """

script_update_customer_stocks = """UPDATE stocks SET current_value = {current_value} 
                                                 WHERE customer_id = {customer_id}"""
