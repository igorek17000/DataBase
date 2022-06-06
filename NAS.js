const api = require('./api/index')
const WSapi = require('./WSapi/index')
const math = require("mathjs");
const telegram = require('./functions/telegram')
require('dotenv').config()
const {
  getCurrentData,
  getEpochData
} = require('./functions/blockChainQueries')

let use_mongo = true
const mongoDB = require('./functions/mongoDB')
/* tIME SERICES DB */
const timeSericeDB = require('./functions/TimeSeriesDB')
/* tIME SERICES DB */


let argv = process.argv
const replica = parseInt(process.env.NAS)

console.log(`Directory name is ${__dirname}`)

let epochId = ''
let epochSubmitEndTime = ''
let epochRevealEndTime = ''
let currentData = ''

let exchangeNoWS = ["bitrue"]

const extractFunc = {
	"binance" : api.binance,
	"binancev1" : api.binancev1,
	"binanceus" : api.binanceus,
	"coinbase" : api.coinbase,
	"bitfinex" : api.bitfinex,
	"ftx" : api.ftx,
	"ftxus" : api.ftxus,
	"huobi" : api.huobi,
	"kraken" : api.kraken,
	"kucoin" : api.kucoin,
	"okex" : api.okex,
	"bitrue" : api.bitrue,
	"gate" : api.gateapi,
	"bybit" : api.bybit,
}

async function getEpoch() {
  [ epochId, epochSubmitEndTime, epochRevealEndTime ] = await getEpochData()
}

async function preparePrice(p) {
	if (p == undefined) return NaN
    let initialFormat = await Number.parseFloat(p.toString()).toFixed(5);
    if (initialFormat.includes('.')) {
      initialFormat = initialFormat.replace('.', '')
    }
    initialFormat = initialFormat.replace(/^0+/, '')
  return initialFormat
}

async function purge() {
  setInterval(async() => {
    console.log("purging")
    let [ epochId, epochSubmitEndTime, epochRevealEndTime ] = await getEpochData()
    for (let i = 4; i < 10; i++)
    {
      console.log("purging "+ (epochId - i))

      switch (argv[2].toLowerCase()) {
        case 'mongodb':
          mongoDB.delete({epochId : (epochId - i).toString()}, "Price NAS")
          break;
        case 'timesericedb':   
        timeSericeDB.Delete("price",{epochId : (epochId - i).toString()})
        break;
        default:
          mongoDB.delete({epochId : (epochId - i).toString()}, "Price NAS")
        }


    }
  }, 1000*60*3)

}

async function itera() {
  let [ epochId, epochSubmitEndTime, epochRevealEndTime ] = await getEpochData()
  currentData = await getCurrentData()
  setInterval(async() => {


	const TimeToSubmit = [...Array(25).keys()].map(i => i + 20)

	const Coins = ["XRP", "LTC", "XLM", "DOGE", "ADA", "ALGO", "BCH", "BTC", "ETH", "FIL", "DGB", "SGB"]
  let source = "WS"
	const exchanges = [
          //"huobi"
					"binance" , "binanceus" ,"bybit" ,
					"coinbase" , "bitfinex" ,"ftx" , "ftxus", "kraken" ,
					"kucoin" ,	"okex" , "bitrue" , "gateio" , "huobi", "okex"
					]

	let exchData = {}
	let coinData = {}
	let coinOB = {}
	let exchObj = {}
	for (let k=0; k<exchanges.length; k++) {
		let newExch = {}
		let newExchObj = {}
		exchData[exchanges[k]] = newExch
		exchObj[exchanges[k]] = newExchObj
	}

    currentData = currentData + 1
    let submitVia = epochSubmitEndTime - currentData
    let revealVia = epochRevealEndTime - currentData
    /*console.log('currentData', currentData)
    console.log('submitVia', submitVia)
    console.log('revealVia', revealVia)*/
    if (revealVia === 1) {

        [ epochId, epochSubmitEndTime, epochRevealEndTime ] = await getEpochData()
				currentData = await getCurrentData()
				console.log('currentData', currentData)
        telegram.message("NAS "+ replica + " exchange Prices recording is Healthy","NAS")
				}
	for (let i=0; i<TimeToSubmit.length; i++) {
		let ts = TimeToSubmit[i]
		if (submitVia === ts) {
      if (source = "WS"){
          for (var ex of exchanges){
            //console.log(ex)

            if ((WSapi[ex] == undefined) && !exchangeNoWS.includes(ex)){
              continue
            }
            for (var cn of Coins){

              let Data = {}
              Data.usdt = 1.0
              Data.epochId = epochId
              Data.timeToSubmission = ts
              Data.exchange = ex
              Data.coin  = cn
              Data.replica = replica

              if (exchangeNoWS.includes(ex)){
                let ticker = await extractFunc[ex].ticker(cn)
                Data.price = ticker.mid
                Data.qV = ticker.volume

              }

              else {
                  if (WSapi[ex].stream[cn] == undefined){
                  continue
                }

              //console.log(cn)
              //console.log(WSapi[ex])

              if (WSapi[ex].unit == "USDT"){
                Data.usdt = (WSapi["kraken"].stream["USDT"].price + WSapi["ftx"].stream["USDT"].price + WSapi["coinbase"].stream["USDT"].price) / 3
              }


              Data.price = WSapi[ex].stream[cn].price
              Data.qV = WSapi[ex].stream[cn].qV
              }

              if(use_mongo){
                switch (argv[2].toLowerCase()) {
                  case 'mongodb':
                    mongoDB.insert(Data, "Price NAS", true)
                    break;
                  case 'timesericedb':   
                  timeSericeDB.InsertData("price",Data)
                    break;
                  default:
                    mongoDB.insert(Data, "Price NAS", true)
                }
              }
              else{
                addApiPrices(Data)
              }
            }
          }
      }
      else if (source == "ccxt") {
        let prices = await api.ccxtr(Coins)
        usdtRate = math.mean(prices.filter((x,i) => x.symbol == "USDT/USD").map((x,i)=> x.price))
        btcRate = math.mean(prices.filter((x,i) => x.symbol == "BTC/USD").map((x,i)=> x.price))
        for (let j=0; j<prices.length; j++) {
          let Data = prices[j]
          Data.epochId = epochId
          Data.timeToSubmission = ts
          if (Data.symbol.split("/")[1] == "USDT")
          {
            Data.price = await preparePrice(prices[j].price*usdtRate)
            Data.priceAW = await preparePrice(prices[j].priceAW*usdtRate)
          }
          else if (Data.symbol.split("/")[1] == "BTC") {
            Data.price = await preparePrice(prices[j].price*btcRate)
            Data.priceAW = await preparePrice(prices[j].priceAW*btcRate)
            Data.qV = Data.qV * usdtRate
          }
          else {
            Data.price = await preparePrice(prices[j].price)
            Data.priceAW = await preparePrice(prices[j].priceAW)
          }
          if(use_mongo){

            switch (argv[2].toLowerCase()) {
              case 'mongodb':
                mongoDB.insert(Data, "Price NAS", true)
                break;
              case 'timesericedb':   
              timeSericeDB.InsertData("price",Data)
                break;
              default:
                mongoDB.insert(Data, "Price NAS", true)
            }

          }
          else{
            addApiPrices(Data)
          }
        }
      }
      else{

			for (let k=0; k<exchanges.length; k++) {
			let exch = exchanges[k]


			let coinDataExch = {}
			coinData[exch] = coinDataExch
			let coinOBExch = {}
			coinOB[exch] = coinOBExch

			for (let j=0; j<Coins.length; j++) {
				let con = Coins[j]
				//console.log(exch, " : " ,extractFunc[exch])
				coinData[exch][con] = extractFunc[exch].price(con) || 0
				coinOB[exch][con] = extractFunc[exch].orderBook(con) || 0
			}

			}
			for (let k=0; k<exchanges.length; k++) {
				let exch = exchanges[k]

					for (let j=0; j<Coins.length; j++) {
  					const con = Coins[j]

  					const priceR = await handleError(coinData[exch][con], api.deftPrice)

  					const OBR = await handleError(coinOB[exch][con], api.deftBook)
  					//console.log(exch, " : data ", con,  " : " , priceR)
  					const price = await preparePrice(priceR)
  					//console.log(exch, " : ", con, " : ", ts, " : ", OBR.bid, " - ", OBR.ask)
  					const bid = await preparePrice(OBR.bid)
  					const ask = await preparePrice(OBR.ask)

  					const Data = {}
  					Data.epochId = epochId
  					Data.timeToSubmission = ts
  					Data.coin = con
  					Data.exchange = exch
  					Data.price = price
  					Data.bid = bid
  					Data.ask = ask

            if(use_mongo){

              switch (argv[2].toLowerCase()) {
                case 'mongodb':
                  mongoDB.insert(Data, "Price NAS", true)
                  break;
                case 'timesericedb':   
                timeSericeDB.InsertData("price",Data)
                  break;
                default:
                  mongoDB.insert(Data, "Price NAS", true)
              }
            }
            else{
              addApiPrices(Data)
            }

				}

      }


			const USDT = await extractFunc["kraken"].price("USDT")
			const USDTBk = await extractFunc["kraken"].orderBook("USDT")
			const Data = {}
			Data.epochId = epochId
			Data.timeToSubmission = ts
			Data.coin = "USDT"
			Data.exchange = "kraken"
			Data.price = await preparePrice(USDT)
			Data.bid = await preparePrice(USDTBk.bid)
			Data.ask = await preparePrice(USDTBk.ask)
      if(use_mongo){

        switch (argv[2].toLowerCase()) {
          case 'mongodb':
            mongoDB.insert(Data, "priceHistory", true)
            break;
          case 'timesericedb':   
          timeSericeDB.InsertData("priceHistory",Data)
          break;
          default:
            mongoDB.insert(Data, "priceHistory", true)
          }

      }
      else{
        addApiPrices(Data)
      }
	   }
	}
  if(use_mongo){

    switch (argv[2].toLowerCase()) {
      case 'mongodb':
        mongoDB.commit("Price NAS")
        break;
      case 'timesericedb':   
        break;
      default:
        mongoDB.commit("Price NAS")
      }

  }
  }
}, 1000)
}


const init = async() => {
  [ epochId, epochSubmitEndTime, epochRevealEndTime ] = await getEpochData()
  currentData = await getCurrentData()
  await getEpoch()
  console.log(epochId)
  console.log('Connecting to DB')
  switch(argv[2].toLowerCase())
  {
    case 'mongodb':
      await mongoDB.connect()
       break;
    case 'timesericedb':   

    await timeSericeDB.connect();
    await timeSericeDB.createTable("price");
    await timeSericeDB.createTable("priceHistory");
    break;
    default:
      await mongoDB.connect()
  }
  if (argv.includes("--delete")){
    mongoDB.delete({}, "Price NAS")
  }
  console.log('Saving prices')
  await purge()
  await itera()

}

init()
