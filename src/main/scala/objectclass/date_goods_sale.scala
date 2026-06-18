package objectclass

class date_goods_sale (datetime:String, goods:String, sales:Double){
  def getDate() = {datetime}
  def getGoods() = {goods}
  def getSales() = {sales}
}