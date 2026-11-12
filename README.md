# mautrix-olx

A Matrix bridge for OLX's messages, on its sites in Bulgaria, Kazakhstan, Poland, Portugal, Romania and Ukraine. It's built on [mautrix-go](https://github.com/mautrix/go)'s bridgev2, like the other mautrix bridges. Each conversation about an advert becomes a DM, named after the person and the advert, with the advert's picture, and its price and link as the topic.

It uses only standard Matrix, so any Matrix client shows it the same way. [Servuso](https://github.com/rafalenden/servuso) bundles it.

## Signing in

Choose your country's sign-in flow and sign in on OLX's own page, as you would on the site. The bridge copies the session the site keeps for itself, including the refresh token it renews itself with every 15 minutes. It also asks for the cookies of `login.olx.<country>`. When the refresh token lapses, as it does after a while unused, such as while a Mac sleeps, the bridge uses them to sign in again silently, as the website does. A client that can't give it those cookies still signs in, but has to sign in again then.

This uses the API behind OLX's website, which OLX doesn't document. It can stop working if OLX changes its site, and using it may break OLX's terms, so it's at your own risk on your own account.

## What works

| | From OLX | To OLX |
|---|---|---|
| Text | Yes | Yes |
| Photos and files | Yes | Yes: JPEG and PNG pictures, PDFs, and Word and Excel files, up to 5 MB, with a caption. They go through the upload service OLX's site uses |
| Chat history | Yes, as much as OLX returns | |
| Marking a chat as read | Yes | Yes |
| Delivered and read | Yes: a message OLX accepted is delivered, and OLX says when it was read | |
| Offers | As text. The other person's carry a link to answer them on OLX | |
| OLX's own lines | As notices from the bridge bot | |

The bridge has no replies, reactions, edits or typing indicators, and chats begin on OLX.

## How it behaves

- **New messages take up to 15 seconds.** The bridge polls the conversation list, and fetches a conversation only when it changed. The website has a live WebSocket that the bridge doesn't use yet.
- **A message sent while signed out is marked not sent**, and the bridge bot says why.
- **Signing out only forgets the session here.** It doesn't sign you out of OLX elsewhere.

## Building

```bash
go build -tags goolm -o mautrix-olx ./cmd/mautrix-olx
```

`goolm` is mautrix-go's own olm, so the binary doesn't need libolm beside it. Run `./mautrix-olx -e` for an example config, and the tests with `go test -tags goolm ./...`.
