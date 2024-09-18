module NotionToKaiten.HttpClient

open System
open System.Net.Http
open System.Net.Http.Json
open System.Threading.Tasks

type TokenBucket(capacity: int, refillRate: double) =
    let mutable tokens = float capacity
    let mutable lastRefillTime = DateTime.UtcNow

    member _.TryConsume() =
        let now = DateTime.UtcNow
        let elapsedSeconds = (now - lastRefillTime).TotalSeconds
        tokens <- min (float capacity) (tokens + elapsedSeconds * refillRate)
        lastRefillTime <- now

        if tokens >= 1.0 then
            tokens <- tokens - 1.0
            true
        else
            false

let private rateLimiter = TokenBucket(5, 5)

let private client = new HttpClient()

let setAuthorizationHeader apiKey =
    client.DefaultRequestHeaders.Add("Authorization", $"Bearer {apiKey}")

let retryWithBackoff (operation: unit -> Task<'T>) =
    let maxAttempts = 3

    let rec retry attempt =
        task {
            try
                while not (rateLimiter.TryConsume()) do
                    do! Task.Delay(200)

                return! operation ()
            with
            | :? HttpRequestException ->
                if attempt < maxAttempts then
                    let delay = Math.Pow(2.0, float attempt) |> int
                    let inc = attempt + 1
                    printfn $"Rate limit reached. Retrying in {delay} seconds. Attempt: {inc}/{maxAttempts}"
                    do! Task.Delay(delay * 1000)
                    return! retry inc
                else
                    return raise (Exception("Max retries reached"))
        }

    retry 0

let get<'T> (url: string) =
    retryWithBackoff (fun () -> client.GetFromJsonAsync<'T>(url))

let put (url: string) (content: MultipartFormDataContent) =
    retryWithBackoff (fun () ->
        task {
            let! response = client.PutAsync(url, content)
            response.EnsureSuccessStatusCode() |> ignore
        })

let patch<'T> (url: string) (content: 'T) =
    retryWithBackoff (fun () ->
        task {
            let! response = client.PatchAsJsonAsync(url, content)
            response.EnsureSuccessStatusCode() |> ignore
        })
