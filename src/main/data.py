# -*- coding:UTF-8 -*-
import random
import string
import sys
import time
from datetime import datetime, timedelta

# 大小写字母
alphabet_upper_list = string.ascii_uppercase
alphabet_lower_list = string.ascii_lowercase

# 随机生成指定位数的字符串
def get_random(instr, length):
    res = random.sample(instr, length)
    result = ''.join(res)
    return result

# 放置生成的并且不存在的rowkey（限制最大缓存10000个，防止内存泄漏）
rowkey_tmp_list = []
MAX_ROWKEY_CACHE = 10000

# 制作rowkey
def get_random_rowkey():
    pre_rowkey = ""
    while True:
        # 获取00~99的两位数字，包含00与99
        num = random.randint(0, 99)
        # 获取当前10位的时间戳
        timestamp = int(time.time())
        # str(num).zfill(2)为字符串不满足2位，自动将该字符串补0
        pre_rowkey = str(num).zfill(2) + str(timestamp)
        if pre_rowkey not in rowkey_tmp_list:
            rowkey_tmp_list.append(pre_rowkey)
            # 超过最大缓存时移除最早的元素
            if len(rowkey_tmp_list) > MAX_ROWKEY_CACHE:
                rowkey_tmp_list.pop(0)
            break
    return pre_rowkey

# 创建用户名
def get_random_name(length):
    name = string.capwords(get_random(alphabet_lower_list, length))
    return name

# 获取年龄
def get_random_age():
    return str(random.randint(18, 60))

# 获取性别
def get_random_sex():
    return random.choice(["woman", "man"])

# 获取商品ID（与你原始列表完全一致）
def get_random_goods_no():
    goods_no_list = ["220902", "430031", "550012", "650012", "532120",
                     "230121", "250983", "480071", "580016", "950013", "152121"]
    return random.choice(goods_no_list)

# 获取商品价格（浮点型，修复精度问题）
def get_random_goods_price():
    price_int = random.randint(1, 999)
    price_decimal = random.randint(1, 99) * 0.01
    # 使用格式化字符串确保价格格式正确，避免科学计数法
    return "{:.2f}".format(price_int + price_decimal)

# 获取门店ID（与你原始列表完全一致）
def get_random_store_id():
    store_id_list = ["313012", "313013", "313014", "313015", "313016",
                     "313017", "313018", "313019", "313020", "313021", "313022", "313023"]
    return random.choice(store_id_list)

# 获取购物行为类型（真实电商分布：pv占比最高，buy最低）
def get_random_goods_type():
    # 权重：pv(65%), cart(15%), fav(7%), buy(8%), scan(5%)
    goods_type_list = ["pv", "pv", "pv", "pv", "pv", "pv", "pv",
                       "cart", "cart",
                       "fav",
                       "buy",
                       "scan"]
    return random.choice(goods_type_list)

# 获取电话号码
def get_random_tel():
    pre_list = ["130", "131", "132", "133", "134", "135", "136", "137", "138", "139",
                "147", "150", "151", "152", "153", "155", "156", "157", "158", "159",
                "186", "187", "188"]
    return random.choice(pre_list) + ''.join(random.sample('0123456789', 8))

# 获取邮箱名
def get_random_email(length):
    alphabet_list = alphabet_lower_list + alphabet_upper_list
    email_list = ["163.com", "126.com", "qq.com", "gmail.com", "huawei.com"]
    return get_random(alphabet_list, length) + "@" + random.choice(email_list)

# 获取商品购买日期（动态生成最近7天，包含当天）
def get_random_buy_time():
    today = datetime.now()
    # 生成最近7天的日期列表
    date_list = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]
    return random.choice(date_list)

# 生成一条数据
def get_random_record():
    return (get_random_rowkey() + "," + get_random_name(5) + "," + get_random_age() + "," +
            get_random_sex() + "," + get_random_goods_no() + "," + get_random_goods_price() + "," +
            get_random_store_id() + "," + get_random_goods_type() + "," + get_random_tel() + "," +
            get_random_email(10) + "," + get_random_buy_time())

# 将记录写到文本中
def write_record_to_file():
    # 覆盖文件内容，重新写入
    with open(sys.argv[1], 'w', encoding='utf-8') as f:
        i = 0
        while i < int(sys.argv[2]):
            record = get_random_record()
            f.write(record + '\n')
            i += 1

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("使用方法: python data.py <输出文件路径> <生成数据条数>")
        print("示例: python data.py /opt/flinkproject/test.log 1000")
        sys.exit(1)
    write_record_to_file()