use redis::Commands;
use redis_streams::{StreamCommands, StreamReadOptions, client_open};

use serde;
use tokio;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct ChatMessage {
    user: u64,
    message: String,
    time_stamp: String,
}


#[tokio::main]
async fn main() {
    println!("Hello from my side");
    let client = client_open("redis://127.0.0.1").expect("Error in connecting with redis");
    let mut conn = client
        .get_connection()
        .expect("Error in getting connection");

    let g : String = conn.xgroup_create("redis_test", "group_yash", "$").unwrap();
    println!("group created : {}",g.to_string());

    let message = ChatMessage {
        user: 111,
        message: "This is the message i am sending from rust redis".to_string(),
        time_stamp: chrono::Local::now().to_string(),
    };

    let serialize = serde_json::to_string(&message).unwrap();

    println!("Serializing data is : {:?}", serialize);

    let s: String = conn
        .xadd("redis_test", "*", &[("data", serialize)])
        .unwrap();

    let thread2 = tokio::spawn(async {
        let client = client_open("redis://127.0.0.1").expect("error in connecting with redis");
        let mut conn = client
            .get_connection()
            .expect("error in getting connection");

        let options = StreamReadOptions::default().group("chat_workers", "worker_1");
        let d = conn
            .xread_options(&["redis_test"], &[">"], options)
            .unwrap();
        println!("{:?}", d);

        for i in d.keys {
            let id = i.ids;
            for data in id {
                let current_id = data.id;
                let d = data.map.get(&"data".to_string()).expect("No data found");

                println!("{:?}", d);
                let json_string = match d {
                    redis_streams::Value::Status(e)=> {
                        let value: ChatMessage = serde_json::from_str(&e).unwrap();
                        println!("Value of the data is : {:?}", value);
                    },
                    redis_streams::Value::Data(e)=>{
                        println!("It is a vec of data {:?}", e);
                        let s = String::from_utf8(e.clone()).unwrap();
                        let value : ChatMessage = serde_json::from_str(&s).unwrap();
                        println!("Message recieved is : {:?}", value);
                    }
                    _ => {
                        println!("Invalid redis value type");
                        continue;
                    }
                };

                let ack : i32 = conn.xack("redis_test", "chat_workers", &[current_id]).unwrap();
                println!("Acknowledge, recieved {}",  ack);
               
            }
        }
    });
}


