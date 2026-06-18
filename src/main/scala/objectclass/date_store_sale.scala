package objectclass

class date_store_sale (datetime:String,store:String,sale:Double){
  def getDate() = {datetime}
  def getStore() = {store}
  def getSale() = {sale}
}