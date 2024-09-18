open System
open System.IO
open System.Net.Http
open Microsoft.Extensions.Configuration
open NotionToKaiten.HttpClient

module KaitenApi =
    let baseUrl = "https://dodopizza.kaiten.ru/api/latest"

    let boardsList spaceId =
        $"%s{baseUrl}/spaces/%s{spaceId}/boards"

    let cardsList columnId offset =
        $"%s{baseUrl}/cards?column_id={columnId}&limit=100&offset={offset}"

    let updateCard cardId = $"%s{baseUrl}/cards/%s{cardId}"

    let attachFile cardId = $"%s{baseUrl}/cards/%s{cardId}/files"


type Lane = { id: int; title: string }

type Column = { id: int; title: string }

type Board =
    { id: int
      title: string
      columns: Column list }

type Card =
    { id: int
      title: string
      description_filled: bool }

type ExportedCard =
    { Lev: Fastenshtein.Levenshtein
      FullPath: string }

let columnCards columnId offset =
    get<Card list> (KaitenApi.cardsList columnId offset)

let cut (title: string) =
    title.Trim()
    |> fun s -> s.Substring(0, min s.Length 50)

let cardName (filePath: string) =
    Path.GetFileNameWithoutExtension filePath
    |> fun x -> x.Substring(0, x.LastIndexOf(' '))
    |> cut

let readCardContent = File.ReadAllTextAsync

let uploadDescription card content =
    patch (KaitenApi.updateCard (string card.id)) {| description = content |}

let attachFileToCard cardId filePath =
    task {
        use content = new MultipartFormDataContent()
        use fileStream = new FileStream(filePath, FileMode.Open)
        use fileContent = new StreamContent(fileStream)
        content.Add(fileContent, "file", Path.GetFileName(filePath))
        do! put (KaitenApi.attachFile (string cardId)) content
    }

let updateCard card cardFile =
    task {
        let! content = readCardContent cardFile
        printfn $"Updated card: %s{card.title}"
        do! uploadDescription card content
        let directory = Path.ChangeExtension(cardFile, null)

        if Directory.Exists(directory) then
            for file in Directory.GetFiles(directory) do
                printfn $"  - Attaching file: %s{file}"
                do! attachFileToCard card.id file
    }

let updateCardDescriptions exportedCards boardCards =
    task {
        for card in boardCards do
            if not card.description_filled then
                let title = card.title |> cut

                let scores =
                    exportedCards
                    |> Array.map (fun x -> x.Lev.DistanceFrom title)

                let distance = Array.min scores

                if distance < 5 then
                    let index = Array.findIndex (fun x -> x = distance) scores
                    let cardFile = exportedCards[index].FullPath
                    do! updateCard card cardFile
                    ()
                else
                    printfn $"{Environment.NewLine}!!! NOT FOUND: %s{card.title}{Environment.NewLine}"
    }

let processColumnCards columnId exportedCards =
    let rec processCards offset totalProcessed =
        task {
            let! cards = get<Card list> (KaitenApi.cardsList columnId offset)
            do! updateCardDescriptions exportedCards cards
            let total = totalProcessed + cards.Length
            printfn $"Processed batch of {cards.Length} cards (Total: {total})"

            match cards.Length with
            | 100 -> return! processCards (offset + 100) total
            | _ -> return total
        }

    processCards 0 0

let fillCards cardFiles space =
    task {
        let! boards = get<Board list> (KaitenApi.boardsList space)

        let exportedCards =
            cardFiles
            |> Array.map (fun x ->
                { Lev = Fastenshtein.Levenshtein(x |> cardName)
                  FullPath = x })

        for board in boards do
            printfn $"> Board: %s{board.title}"

            for column in board.columns do
                printfn $"> Column: %s{column.title}"
                let! totalProcessed = processColumnCards column.id exportedCards
                printfn $"> Finished processing {totalProcessed} cards for column \"{column.title}\""
    }

type private _marker() =
    class
    end

[<EntryPoint>]
let main args =
    let config =
        ConfigurationBuilder()
            .AddUserSecrets<_marker>()
            .AddCommandLine(args)
            .Build()

    let apiKey = config["apikey"]
    let space = config["space"]
    let sourceDirectory = config["sourceDirectory"]

    let cardFiles = Directory.GetFiles(sourceDirectory, "*.md")
    setAuthorizationHeader apiKey

    fillCards cardFiles space
    |> Async.AwaitTask
    |> Async.RunSynchronously

    0
