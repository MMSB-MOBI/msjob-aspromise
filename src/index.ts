
import * as jobManagerClient from 'ms-jobmanager'

export class PromiseManager {
    private port: number;
    private TCPip: string;
    private _connect: boolean = false;

    constructor(adress : string, port: number) {

        this.port = port
        this.TCPip = adress
    }

    async start() {

        return new Promise((res, rej) => {
            if (this._connect) res(true)
            else{
                jobManagerClient.start({ port: this.port, TCPip: this.TCPip }).then( (disconnectEmitter :any) => {
                    this._connect = true
                    disconnectEmitter.on("disconnect", () => {
                        this._connect = false
                    })
                    res(true)
                }).catch( (e :any) => {
                    console.error(`Unable to connect at ${this.TCPip}:${this.port} : ${e}`)
                    rej(e)
                })
            }
           
        })
    }

    async push(jobOpt: jobManagerClient.jobOptProxyClient): Promise<any> { // Guillaume doit typer l'objet job

        return new Promise((res, rej) => {
            this.start().then(() => {
                const job = jobManagerClient.push(jobOpt);
                job.on("scriptError", (message: string, data:jobManagerClient.jobOptProxyClient) => {
                    console.error("script error");
                    
                    rej(`Error with job ${job.id} : script error`)
                });
                job.on("lostJob", (data:jobManagerClient.jobOptProxyClient) => {
                    //console.log("lost job", data);
                    rej(`Error with job ${job.id} : job has been lost`)
                });

                
                job.on("fsFatalError", (message: string, error: string, data:jobManagerClient.jobOptProxyClient) => { //jobObject
                    console.log("fs fatal error");
                    console.log("message:", message);
                    console.log("error:", error);
                    console.log("data:", data);
                    rej(`Error with job ${job.id} : fsFatalError`)
                });
                job.on("scriptSetPermissionError", (err: string) => {
                    console.error(err)
                    rej(`Error with job ${job.id} : script permission error`)
                });
                job.on("scriptWriteError", (err: string) => {
                    console.error("scriptWriteError", err)
                    rej(`Error with job ${job.id} : script write error`)
                });
                job.on("scriptReadError", (err: string) => {
                    console.error("script read error", err);
                    rej(`Error with job ${job.id} : script read error`)
                });
                job.on("inputError", (err: string) => {
                    console.error("input error", err);
                    rej(`Error with job ${job.id} : input error`)
                });

                job.on("disconnect_error",() => {
                    console.error("job disconnected")
                    rej(`Error with job ${job.id} : disconnect error`)
                })

                job.on("completed", (stdout: any, stderr: any) => {
                    const chunks: Uint8Array[] = [];
                    const errchunks: Uint8Array[] = [];
                    console.log("STDOUT");
                    stdout.on('data', (chunk: Uint8Array) => chunks.push(chunk))
                    stdout.on('end', () => {
                        const _ = Buffer.concat(chunks).toString('utf8');
                        try {
                            const data = JSON.parse(_)
                            res(data);
                        } catch (e) {
                            rej(e);
                        }
                    });
                    stderr.on('data', (chunk: Uint8Array) => errchunks.push(chunk))
                    stderr.on('end', () => {
                        if (errchunks) {
                            const _ = Buffer.concat(errchunks).toString('utf8');
                            console.log(`erreur standard job>${_}<`);
                            if (_) rej(_)
                        }
                    })
                })
            })
            .catch(e => {
                rej(`Job manager error : ${e}`)
            })
        })
    }
}