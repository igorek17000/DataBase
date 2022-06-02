
const {Client}=require("pg")
const client=new Client({
    host:"ap61ezh076.ur5pwmx6yc.tsdb.cloud.timescale.com",
    user:"tsdbadmin",
    port:37195,
    password:'o34qedsmw23mem1j',
    database:"tsdb",
})
let connect=async()=>{
    try{
    await client.connect()
}catch(e){
    console.log(e)
}
}
let createTable=async(tableName)=>{
    try{
        await  client.query( `
        CREATE TABLE IF NOT EXISTS ${tableName}(
          epochId integer,
          usdt  float(20),
          coin  VARCHAR (10),
          exchange  VARCHAR (20),
          timeToSubmission integer,
          replica VARCHAR(20),
          price float(20),
          qV float(20)
        );`)
    }catch (e){
        console.log(e)
    }
}
let InsertData=async(tableName,Data)=>{
    try{
       await  client.query(`INSERT INTO ${tableName}
        (
        epochId ,
        usdt,
        coin,
        exchange,
        timeToSubmission ,
        replica ,
        price ,
        qV )
        VALUES(${Data.epochId},${Data.usdt},'${Data.coin}','${Data.exchange}',
               ${Data.timeToSubmission},'${Data.replica}',${Data.price},${Data.qV})`)          

    }catch(e){
        console.log()
    }
}
let Delete=async(tabeleName,condition)=>{

    try{
await client.query(`DELETE FROM ${tabeleName}
WHERE ${condition};`)
    }catch(e){

    }
}
module.exports = {connect: connect, InsertData : InsertData, createTable : createTable,Delete:Delete}
