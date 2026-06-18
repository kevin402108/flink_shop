from flask import Flask, jsonify
from flask_cors import CORS
import pymysql

app = Flask(__name__)
CORS(app)  # 解决跨域问题

# 数据库配置（与你实训文档完全一致）
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'fk_shop',
    'charset': 'utf8mb4'
}


def get_db_connection():
    return pymysql.connect(**DB_CONFIG)


@app.route('/api/dashboard', methods=['GET'])
def get_dashboard_data():
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    try:
        # ====================== 1. 核心指标（完全匹配你的数据）======================
        # 总访问量（所有行为都算访问）
        cursor.execute("SELECT IFNULL(SUM(visitcount), 0) as total_pv FROM visitcount_everyday")
        total_pv = cursor.fetchone()['total_pv']

        # 总销售额（仅buy行为）
        cursor.execute("SELECT IFNULL(SUM(salevolume), 0) as total_sale FROM salevolume")
        total_sale = cursor.fetchone()['total_sale']

        # 总购买订单数
        cursor.execute("SELECT IFNULL(COUNT(*), 0) as buy_count FROM store_sale")
        buy_count = cursor.fetchone()['buy_count'] or 1

        # 计算转化率和客单价
        buy_rate = round((buy_count / total_pv) * 100, 2) if total_pv > 0 else 0
        avg_price = round(total_sale / buy_count, 2) if buy_count > 0 else 0

        # ====================== 2. 近7天趋势（匹配你的日期范围：6.08-6.14）======================
        cursor.execute("""
            SELECT v.datetime, v.visitcount, IFNULL(s.salevolume, 0) as salevolume 
            FROM visitcount_everyday v
            LEFT JOIN salevolume s ON v.datetime = s.datetime
            ORDER BY v.datetime DESC 
            LIMIT 7
        """)
        trend_data = cursor.fetchall()
        trend_data.reverse()  # 按日期正序排列（6.08 → 6.14）

        # ==========
        # ============ 3. 各门店销售额（匹配你的12个门店ID）======================
        cursor.execute("""
            SELECT store_id, IFNULL(SUM(sale), 0) as total_sale 
            FROM store_sale 
            GROUP BY store_id 
            ORDER BY total_sale DESC
        """)
        store_sale_data = cursor.fetchall()

        # ====================== 4. 商品销量Top10（匹配你的12个商品ID）======================
        cursor.execute("""
            SELECT goods_id, IFNULL(SUM(sales), 0) as total_sales 
            FROM goods_sale 
            GROUP BY goods_id 
            ORDER BY total_sales DESC 
            LIMIT 10
        """)
        goods_top_data = cursor.fetchall()
        goods_top = [{'goodsId': item['goods_id'], 'sales': int(item['total_sales'])} for item in goods_top_data]

        # ====================== 5. 门店访问量Top10（匹配你的12个门店ID）======================
        cursor.execute("""
            SELECT store_id, IFNULL(SUM(visits), 0) as total_visits 
            FROM store_visit 
            GROUP BY store_id 
            ORDER BY total_visits DESC 
            LIMIT 10
        """)
        store_top_data = cursor.fetchall()
        store_top = [{'storeId': item['store_id'], 'visits': int(item['total_visits'])} for item in store_top_data]

        # ====================== 6. 用户行为占比（匹配你的5种行为类型）======================
        cursor.execute("""
            SELECT behavior_type, IFNULL(SUM(count), 0) as total_count 
            FROM behavior_count 
            GROUP BY behavior_type
        """)
        behavior_data_raw = cursor.fetchall()

        # 补全所有5种行为类型（防止某类行为暂无数据）
        behavior_map = {'pv': 0, 'buy': 0, 'cart': 0, 'fav': 0, 'scan': 0}
        for item in behavior_data_raw:
            if item['behavior_type'] in behavior_map:
                behavior_map[item['behavior_type']] = int(item['total_count'])

        # 计算百分比
        total_behavior = sum(behavior_map.values()) or 1
        behavior_data = [
            {'name': '浏览(pv)', 'value': round(behavior_map['pv'] / total_behavior * 100, 1)},
            {'name': '购买(buy)', 'value': round(behavior_map['buy'] / total_behavior * 100, 1)},
            {'name': '加购(cart)', 'value': round(behavior_map['cart'] / total_behavior * 100, 1)},
            {'name': '收藏(fav)', 'value': round(behavior_map['fav'] / total_behavior * 100, 1)},
            {'name': '扫码(scan)', 'value': round(behavior_map['scan'] / total_behavior * 100, 1)}
        ]

        # ====================== 7. 转化率（动态计算）======================
        cart_rate = round(behavior_map['cart'] / total_behavior * 100, 2) if total_behavior > 0 else 0
        fav_rate = round(behavior_map['fav'] / total_behavior * 100, 2) if total_behavior > 0 else 0

        # ====================== 返回结果 ======================
        result = {
            'overview': {
                'totalPv': int(total_pv),
                'totalSale': round(total_sale, 2),
                'buyRate': buy_rate,
                'avgPrice': avg_price
            },
            'trend': {
                'dates': [item['datetime'] for item in trend_data],
                'pv': [int(item['visitcount']) for item in trend_data],
                'sale': [round(item['salevolume'], 2) for item in trend_data]
            },
            'behavior': behavior_data,
            'storeSale': [{'storeId': item['store_id'], 'sale': round(item['total_sale'], 2)} for item in
                          store_sale_data],
            'goodsTop': goods_top,
            'storeTop': store_top,
            'conversion': {
                'cartRate': cart_rate,
                'favRate': fav_rate,
                'buyRate': buy_rate
            }
        }

        return jsonify(result)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    # 允许所有IP访问，端口5000
    app.run(host='0.0.0.0', port=5000, debug=True)