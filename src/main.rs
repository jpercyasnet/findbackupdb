mod connectbk;
use connectbk::connectbk;
use rusqlite::{Connection, Result};
use std::path::{Path};
use std::io::{Write, BufRead, BufReader};
use std::fs::File;
use std::env;
use std::process::Command as stdCommand;
use std::time::Instant as timeInstant;
use chrono::Local;
#[derive(Debug)]
struct Bkup {
      rowid: u64,
      refname: String,
      filename: String,
      dirname: String,
      filesize: u64,
      filedate: String,
      md5sum: Option<String>,
      locations: Option<String>,
      notes: Option<String>,
}

fn main() -> Result<()> {
    let mut bolok = true;
    let mut parm1dir = String::new();
    let mut parm2dir = String::new();
    let mut outseq: u32 = 1;

    let args: Vec<_> = env::args().collect();
    if args.len() < 2 {
        println!(" no input parameters; need bkdatabase, windirparse");
        bolok = false;
    } else {
        println!("The first argument is {}", args[1]);
        if args.len() < 3 {
            println!("The Only first argument and no winddirparse");
            bolok = false;
        } else {
            println!("The second argument is {}", args[2]);
            if !Path::new(&args[1]).exists() {
                println!("The first argument {} does not exist", args[1]);
                bolok = false;
            } else {
                println!("The first argument {} exists", args[1]);
                parm1dir = args[1].to_string();
                let conn1 = Connection::open(parm1dir.clone()).unwrap();
                if let Err(e) = connectbk(&conn1) {
                    println!("data base for backup error: {}", e);
                    bolok = false;
                } else {
                    println!("data base good for backup");
                }
            }
        }
    }
    if bolok {
        if !Path::new(&args[2]).exists() {
            println!("The second argument {} does not exist", args[2]);
            bolok = false;
        } else {
            println!("The second argument {} exists", args[2]);
            parm2dir = args[2].to_string();
            let outputx = stdCommand::new("wc")
                         .arg("-l")
                         .arg(&parm2dir)
                         .output()
                         .expect("failed to execute process");
            let stroutx = String::from_utf8_lossy(&outputx.stdout);
            let vecout: Vec<&str> = stroutx.split(" ").collect();
            let numlinesx: i64 = vecout[0].parse().unwrap_or(-9999);
            if numlinesx == -9999 {
                println!("size of {} is invalid for wc -l command call", vecout[0]);
                bolok = false;
            } else {
                let rows_num = numlinesx as u64;
                if rows_num < 2 {
                    println!("size of {} is less than 2 for {}", rows_num, parm2dir);
                    bolok = false;
                } else {
                    let file = File::open(parm2dir.clone()).unwrap();
                    let mut reader = BufReader::new(file);
                    let mut linehd = String::new();
                    bolok = false;
                    loop {
                        match reader.read_line(&mut linehd) {
                             Ok(bytes_read) => {
                                 if bytes_read == 0 {
                                     println!("error bytes_read == 0 for {}", parm2dir);
                                     break;
                                 }
                                 let cnt = linehd.matches("|").count();
                                 if cnt != 4 {
                                     println!("first line of windirparse file is not valid: {}", linehd);
                                 } else {
                                     println!("windirparse file is ok with size of {} rows", rows_num);
                                     bolok = true;
                                 }
                                 break;
                             }
                             Err(err) => {  
                                 println!("error of {} reading {}", err, parm2dir);
                                 break;
                             }
                        };
                    }
                }
            }
        }
    }
    if bolok {
        let mut more1out: String = format!("./more1{:02}.excout", outseq);
        let mut just1out: String = format!("./just1{:02}.neout", outseq);
        let mut diffdateout: String = format!("./diffdate{:02}.excout", outseq);
        let mut nobkupout: String = format!("./nobkup{:02}.neout", outseq);
        let mut errout: String = format!("./generrors{:02}.errout", outseq);
        loop {
               if !Path::new(&errout).exists() && !Path::new(&more1out).exists() && !Path::new(&just1out).exists()
                  && !Path::new(&diffdateout).exists() && !Path::new(&nobkupout).exists() {
                   break;
               } else {
                   outseq = outseq + 1;
                   more1out = format!("./more1{:02}.excout", outseq);
                   just1out = format!("./just1{:02}.neout", outseq);
                   diffdateout = format!("./diffdate{:02}.excout", outseq);
                   nobkupout = format!("./nobkup{:02}.neout", outseq);
                   errout = format!("./generrors{:02}.errout", outseq);
               }
        }          
        let conndb = Connection::open(parm1dir.clone()).unwrap();
        let mut diffdatefile = File::create(diffdateout).unwrap();
        let mut nobkupfile = File::create(nobkupout).unwrap();
        let mut more1file = File::create(more1out).unwrap();
        let mut just1file = File::create(just1out).unwrap();
        let mut errfile = File::create(errout).unwrap();
        let filex = File::open(parm2dir.clone()).unwrap();
        let mut readerx = BufReader::new(filex);
        let mut linex = String::new();
        let mut line1000: u64 = 0;
        let mut linenumx: u64 = 0;
        let start_time = timeInstant::now();

        loop {
              match readerx.read_line(&mut linex) {
                 Ok(bytes_read) => {
                 // EOF: save last file address to restart from this address for next run
                     if bytes_read == 0 {
                         break;
                     }
                     line1000 = line1000 + 1;
                     linenumx = linenumx + 1;
                     if line1000 > 20 {
                         let diffy = start_time.elapsed();
                         let minsy: f64 = diffy.as_secs() as f64/60 as f64;
                         let dateyy = Local::now();
                         println!("line number {} records elapsed time {:.1} mins at {}", linenumx, minsy, dateyy.format("%H:%M:%S"));
                         line1000 = 0;
                     }
                     let vecline: Vec<&str> = linex.split("|").collect();
                     let inptdir = vecline[1].to_string();
                     let inptsize: String = vecline[3].to_string();
                     let inptdate: String = format!("{}.000", vecline[2]);
                     let mut inptfilenm: String = vecline[0].to_string();
                     if inptfilenm[..1].to_string() == '"'.to_string() {
                         inptfilenm = inptfilenm[1..(inptfilenm.len()-1)].to_string();
                     }
                     match conndb.prepare("SELECT  rowid, refname, filename, dirname, filesize, filedate, md5sum, locations, notes
                                          FROM blubackup
                                          WHERE filename = :fil") {
                          Err(err) => {
                               writeln!(&mut errfile, "err {} in sql prepare call for file {}", err, inptfilenm).unwrap();
                          }
                          Ok(mut stmt) => {
                              match stmt.query_map(&[(":fil", &inptfilenm)], |row| {
                                      Ok(Bkup {
                                            rowid: row.get(0)?,
                                            refname: row.get(1)?,
                                            filename: row.get(2)?,
                                            dirname: row.get(3)?,
                                            filesize: row.get(4)?,
                                            filedate: row.get(5)?,
                                            md5sum: row.get(6)?,
                                            locations: row.get(7)?,
                                            notes: row.get(8)?,
                                      })
                                    })
                                  {
                                    Err(err) => {
                                        writeln!(&mut errfile, "err {} in sql query for file {}", err, inptfilenm).unwrap();
                                    }
                                    Ok(bk_iter) => {
                                        let mut numentries = 0;
                                        let mut numdate = 0;
                                        let mut numsize = 0;
                                        for bk in bk_iter {
                                             numentries = numentries + 1;
                                             let bki = bk.unwrap();
                                             let bksize = format!("{}", bki.filesize);
                                             let bkdir: String = bki.dirname;
                                             let bkdate: String = bki.filedate;
                                             let bkref: String = bki.refname;
                                             let stroutput = format!("{}|{}|{}|{}|{}|{}|{}|{}", 
                                                              bkref, bkdir, inptfilenm, bksize, inptsize, bkdate, inptdate, inptdir);
                                             if bksize == inptsize {
                                                 numsize = numsize + 1;
                                                 if bkdate == inptdate {
                                                     numdate = numdate + 1;
                                                     if numdate > 1 {
                                                         writeln!(&mut more1file, "{}", stroutput).unwrap();
                                                     } else {
                                                         writeln!(&mut just1file, "{}", stroutput).unwrap();
                                                     }
                                                 } else {
                                                     writeln!(&mut diffdatefile, "{}", stroutput).unwrap();
                                                 }
                                             }
                                        }
                                        if numentries < 1 {
                                            let stroutput: String = format!("{} -{}- -{}-", linenumx, linex, inptfilenm);
                                            writeln!(&mut nobkupfile, "{}", stroutput).unwrap();
                                        } else {
                                            if numsize < 1 {
                                                let stroutput: String = format!("{} NO MATCHING SIZE -{}- -{}-", linenumx, linex, inptfilenm);
                                                writeln!(&mut errfile, "{}", stroutput).unwrap();
                                            } else {
                                                if numdate < 1 {
                                                    let stroutput: String = format!("{} NO MATCHING DATE -{}- -{}-", linenumx, linex, inptfilenm);
                                                    writeln!(&mut errfile, "{}", stroutput).unwrap();
                                                }
                                            }
                                        }
                                    }
                                  }
                          } // Ok
                     } // match conn
                     linex.clear();
                 }
                 Err(err) => {
                     println!("read error {:?}", err);
                     break;
                 }
              };
        }
        let diffy = start_time.elapsed();
        let minsy: f64 = diffy.as_secs() as f64/60 as f64;
        let dateyy = Local::now();
        println!("{} files elapsed time {:.1} mins at {}", linenumx, minsy, dateyy.format("%H:%M:%S"));
    }
    Ok(())
}
