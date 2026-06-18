package objectclass

class date_store_visit (datetime:String, store:String, visits:Double){
  def getDate() = {datetime}
  def getStore() = {store}
  def getVisits() = {visits}
}