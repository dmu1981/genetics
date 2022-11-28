#![allow(dead_code)]
use amiquip::{
    Channel, Connection, Delivery, Exchange, Get,
    Publish, QueueDeclareOptions,
};

use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub struct GenomeMessage<T>
where
    T: Serialize,
{
    pub uuid: Uuid,
    pub fitness: Option<f32>,
    pub generation: u32,
    pub payload: T,
}

pub struct GenomeAck<T>
where
    T: Serialize,
{
    channel: Channel,
    delivery: Delivery,
    pub message: GenomeMessage<T>,
}

pub struct Genome<T: Serialize + for<'a> Deserialize<'a>> {
    
    pub message: GenomeMessage<T>,
}

impl<T: Serialize + for<'a> Deserialize<'a>> Genome<T> {
    pub fn new(generation: u32, payload: T) -> Genome<T> {
        Genome {
            message: GenomeMessage {
                fitness: None,
                generation,
                uuid: Uuid::new_v4(),
                payload,
            },
        }
    }

    pub fn sync(message: GenomeMessage<T>) -> Genome<T> {
        Genome { message }
    }
}

pub type PopulationHandler<T> = fn(genes: &Vec<Genome<T>>) -> Result<Vec<Genome<T>>, GenomeError>;

pub enum FitnessSortingOrder {
  LessIsBetter,
  MoreIsBetter,
}

pub struct GenePool<T: Serialize + for<'a> Deserialize<'a>> {
    sorting_order: FitnessSortingOrder,
    pub population_size: u32,
    rmq_url: String,
    rmq_pending_queue: String,
    rmq_ready_queue: String,
    rmq_best_queue: String,
    connection: Connection,
    pub genes: Option<Vec<Genome<T>>>,
}

#[derive(Debug)]
pub struct GenomeError {}

impl From<amiquip::Error> for GenomeError {
    fn from(_: amiquip::Error) -> GenomeError {
        GenomeError {}
    }
}

impl From<serde_json::Error> for GenomeError {
    fn from(_: serde_json::Error) -> GenomeError {
        GenomeError {}
    }
}

impl<T: Serialize + for<'a> Deserialize<'a>> GenePool<T> {
    pub fn new(population_size: u32, 
               sorting_order: FitnessSortingOrder,
               url: String
               ) -> Result<GenePool<T>, GenomeError> {
        //let url = "amqp://guest:guest@192.168.178.44:5672".to_owned();
        let pending_queue = "pending_genomes".to_owned();
        let ready_queue = "ready_genomes".to_owned();
        let best_queue = "best_genomes".to_owned();
        // Open connection.
        let connection = Connection::insecure_open(&url)?;

        Ok(GenePool {
            sorting_order,
            population_size,
            rmq_url: url,
            rmq_pending_queue: pending_queue,
            rmq_ready_queue: ready_queue,
            rmq_best_queue: best_queue,
            genes: None,
            connection,
        })
    }

    fn open_channel(&mut self) -> Result<Channel, GenomeError> {
        loop {
            match self.connection.open_channel(None) {
                Ok(ch) => {
                    return Ok(ch);
                }
                Err(err) => match err {
                    amiquip::Error::EventLoopDropped => {
                        println!("EventLoopDropped... reconnecting");
                        //tokio::time::sleep(Duration::from_millis(1500)).await;
                        self.connection = Connection::insecure_open(&self.rmq_url).unwrap();
                        continue;
                    }
                    _ => {
                        return Err(GenomeError {});
                    }
                },
            }
        }
    }

    pub fn empty_pool(&mut self) -> Result<(), GenomeError> {
        // Start over again with a new genes vector
        self.genes = None;

        // Open a channel - None says let the library choose the channel ID.
        let channel = self.open_channel()?; //self.connection.open_channel(None).unwrap();

        // Declare the "hello" queue.
        {
          let mut options = QueueDeclareOptions::default();
          options.durable = true;

          let queue =
              channel.queue_declare(&self.rmq_pending_queue, options)?;

          queue.purge()?;
        }

        {
          let mut options = QueueDeclareOptions::default();
          options.durable = true;

          let queue2 =
              channel.queue_declare(&self.rmq_ready_queue, options)?;

          queue2.purge()?;
        }

        {
          let mut options = QueueDeclareOptions::default();
          options.durable = true;

          let queue3 =
              channel.queue_declare(&self.rmq_best_queue, options)?;

          queue3.purge()?;
        }

        Ok(())
    }

   fn push_to_queue(
        &mut self,
        queue: &String,
        genome: &GenomeMessage<T>,
    ) -> Result<(), GenomeError> {
        // Open a channel - None says let the library choose the channel ID.
        let channel = self.open_channel()?; //self.connection.open_channel(None).unwrap();

        // Create an exchange
        let exchange = Exchange::direct(&channel);

        // Serialize the payload
        let payload = serde_json::to_string(&genome)?;
        //println!("Pushing {} to {}", payload, queue);

        // Publish a message to the "hello" queue.
        //exchange
        //exchange.publish(Publish::new(payload.as_bytes(), queue, ))?;
        let properties = amiquip::AmqpProperties::default().with_delivery_mode(2);
        exchange.publish(Publish::with_properties(payload.as_bytes(), queue, properties))?;

        Ok(())
        //connection.close().unwrap();
    }

    pub async fn add_genome(&mut self, genome: Genome<T>) -> Result<(), GenomeError> {
        match self.genes {
            Some(_) => {}
            None => {
                self.genes = Some(Vec::<Genome<T>>::new());
            }
        }
        // Mark the genome as pending
        //genome.status = GenomeStatus::Pending;

        // Push into queue
        self.push_to_queue(&self.rmq_pending_queue.clone(), &genome.message)
            ?;

        // Push into our internal list so we can track it
        self.genes.as_mut().unwrap().push(genome);

        Ok(())
    }

    pub fn ack_one(&mut self, gene: GenomeAck<T>) -> Result<(), GenomeError> {
        self.push_to_queue(&self.rmq_ready_queue.clone(), &gene.message)
            ?;

        // If the program crashes here, we will have a dupliacated message in both queues

        gene.delivery.ack(&gene.channel)?;

        Ok(())
    }

    pub fn poll_one(&mut self) -> Result<GenomeAck<T>, GenomeError> {
        // Open a channel - None says let the library choose the channel ID.*/
        let channel = self.open_channel()?;

        let res: Option<Get>;
        {
            // Declare the "hello" queue.
            let mut options = QueueDeclareOptions::default();
            options.durable = true;

            let queue =
                channel.queue_declare(&self.rmq_pending_queue, options)?;

            res = queue.get(false)?;
        }

        match res {
            Some(msg) => {
                let body = String::from_utf8_lossy(&msg.delivery.body);
                let genome: GenomeMessage<T> = serde_json::from_str(&body)?;

                Ok(GenomeAck {
                    channel,
                    delivery: msg.delivery,
                    message: genome,
                })
            }
            None => Err(GenomeError {}),
        }
    }

    pub async fn monitor(
        &mut self,
        population_handler: PopulationHandler<T>,
    ) -> Result<(), GenomeError> {
        self.genes = Some(Vec::<Genome<T>>::new());

        // Open a channel - None says let the library choose the channel ID.*/
        let channel = self.open_channel()?;

        let mut deliveries = Vec::<Delivery>::new();

        loop {
            let res: Option<Get>;
            {
                let mut options = QueueDeclareOptions::default();
                options.durable = true;

                // Declare the "hello" queue.
                let queue =
                    channel.queue_declare(&self.rmq_ready_queue, options)?;

                res = queue.get(false)?;
            }

            match res {
                Some(msg) => {
                    let body = String::from_utf8_lossy(&msg.delivery.body);
                    //println!("{:?}", body);
                    let genome: GenomeMessage<T> = serde_json::from_str(&body)?;

                    deliveries.push(msg.delivery);

                    tokio::time::sleep(Duration::from_millis(5)).await;

                    match genome.fitness {
                      Some(fitness) => {
                        println!(
                          "Syncing Genome {} out of {} into pool... Generation {}, fitness was {}",
                          deliveries.len(),
                          self.population_size,
                          genome.generation,
                          fitness
                        );
                      },
                      None => {
                        println!("Syncing Genome without fitness into pool, something was wrong!");
                      }
                    }                   
                    

                    self.genes
                        .as_mut()
                        .unwrap()
                        .push(Genome::<T>::sync(genome));

                    if deliveries.len() as u32 == self.population_size {
                        { 
                            let mut genes = self.genes.take().unwrap();
                            /*let genes: &mut Vec<Genome<T>>;
                            {
                              let binding = self.genes.as_mut().expect("Should have genes");
                              genes = binding.as_mut();
                            }*/
                            genes.sort_by(|a, b| 
                            a.message.fitness.unwrap_or(99999.0).partial_cmp(&b.message.fitness.unwrap_or(99999.0)).unwrap_or(std::cmp::Ordering::Equal));
                            if let FitnessSortingOrder::MoreIsBetter = self.sorting_order {
                              genes.reverse();
                            }

                            self.push_to_queue(&self.rmq_best_queue.clone(), &genes[0].message)?;

                            let new_genes = population_handler(&genes)?;

                            //self.empty_pool().await?;

                            self.genes = None;
                            for gene in new_genes.into_iter() {
                                self.add_genome(gene).await?;
                            }

                            for d in deliveries.into_iter() {
                                d.ack(&channel)?;
                            }
                        }

                        deliveries = Vec::<Delivery>::new();
                    }
                }
                None => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }
}

unsafe impl<T> Send for GenePool<T> where T: Serialize + for<'a> Deserialize<'a> {}



