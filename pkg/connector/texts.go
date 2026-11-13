package connector

import (
	"fmt"
	"strings"
	"unicode"
)

// texts is what the bridge itself writes into a chat. It's in the language of
// the OLX site the account is on, as OLX's own lines in the chat are, rather
// than English beside them.
type texts struct {
	// Language is the language's code, kept so that what was written in another
	// language can be written again.
	Language string
	// Respond is the link under an offer that's answered on OLX's site.
	Respond string
	// Rejected and Accepted say what became of an offer.
	Rejected string
	Accepted string
	// NoContent stands in for a message with nothing that can be shown.
	NoContent string
	// AttachmentFailed stands in for a file that couldn't be fetched, with %s
	// where its name goes.
	AttachmentFailed string
}

var english = texts{
	Language:         "en",
	Respond:          "Respond on OLX",
	Rejected:         "Rejected",
	Accepted:         "Accepted",
	NoContent:        "This message has no content that can be shown here.",
	AttachmentFailed: "Couldn't load the attachment %s",
}

var ukrainian = texts{
	Language:         "uk",
	Respond:          "Відповісти на OLX",
	Rejected:         "Відхилено",
	Accepted:         "Прийнято",
	NoContent:        "Це повідомлення неможливо показати тут.",
	AttachmentFailed: "Не вдалося завантажити вкладення %s",
}

var russian = texts{
	Language:         "ru",
	Respond:          "Ответить на OLX",
	Rejected:         "Отклонено",
	Accepted:         "Принято",
	NoContent:        "Это сообщение невозможно показать здесь.",
	AttachmentFailed: "Не удалось загрузить вложение %s",
}

var kazakh = texts{
	Language:         "kk",
	Respond:          "OLX-те жауап беру",
	Rejected:         "Қабылданбады",
	Accepted:         "Қабылданды",
	NoContent:        "Бұл хабарды мұнда көрсету мүмкін емес.",
	AttachmentFailed: "%s тіркемесін жүктеу мүмкін болмады",
}

// Each site's own language, by the end of its address. Ukraine's opens in
// Ukrainian and Kazakhstan's in Russian. "Rejected" and "Accepted" are OLX's
// own words where the site has them.
var siteTexts = map[string]texts{
	".pl": {
		Language:         "pl",
		Respond:          "Odpowiedz na OLX",
		Rejected:         "Odrzucono",
		Accepted:         "Zaakceptowano",
		NoContent:        "Tej wiadomości nie da się tutaj wyświetlić.",
		AttachmentFailed: "Nie udało się wczytać załącznika %s",
	},
	".ro": {
		Language:         "ro",
		Respond:          "Răspunde pe OLX",
		Rejected:         "Respinsă",
		Accepted:         "Acceptată",
		NoContent:        "Acest mesaj nu poate fi afișat aici.",
		AttachmentFailed: "Atașamentul %s nu a putut fi încărcat",
	},
	".bg": {
		Language:         "bg",
		Respond:          "Отговорете в OLX",
		Rejected:         "Отхвърлено",
		Accepted:         "Прието",
		NoContent:        "Това съобщение не може да бъде показано тук.",
		AttachmentFailed: "Прикаченият файл %s не можа да се зареди",
	},
	".pt": {
		Language:         "pt",
		Respond:          "Responder no OLX",
		Rejected:         "Rejeitada",
		Accepted:         "Aceite",
		NoContent:        "Esta mensagem não pode ser mostrada aqui.",
		AttachmentFailed: "Não foi possível carregar o anexo %s",
	},
	".ua": ukrainian,
	".kz": russian,
}

// siteWords is a site's texts: those of its own language and, on a site people
// use in two, those of both.
type siteWords struct {
	// The site's own language, for what stands on its own.
	texts
	// The letters only one of a site's two languages uses, that language, and
	// the other one.
	letters       string
	marked, plain texts
}

// The sites people use in two languages. OLX writes an offer's line in the
// language of whoever made it, so what the bridge adds under it follows the
// line: Ukrainian and Kazakh each have letters Russian doesn't.
var twoLanguages = map[string]siteWords{
	".ua": {letters: "іїєґ", marked: ukrainian, plain: russian},
	".kz": {letters: "әғқңөұүһі", marked: kazakh, plain: russian},
}

// textsFor picks the languages of a site such as www.olx.pl. Any other address
// gets English.
func textsFor(site string) siteWords {
	site = strings.TrimRight(strings.TrimSpace(site), "/")
	for suffix, found := range siteTexts {
		if strings.HasSuffix(site, suffix) {
			words := twoLanguages[suffix]
			words.texts = found
			return words
		}
	}
	return siteWords{texts: english}
}

// under is the texts to put under a line OLX wrote: in the line's language where
// the site has two, and the site's own if the line doesn't tell, as one with
// only a price in it.
func (s siteWords) under(line string) texts {
	if s.letters == "" {
		return s.texts
	}
	line = strings.ToLower(line)
	if strings.ContainsAny(line, s.letters) {
		return s.marked
	}
	if strings.ContainsFunc(line, func(r rune) bool { return unicode.Is(unicode.Cyrillic, r) }) {
		return s.plain
	}
	return s.texts
}

// attachmentFailed names a file that couldn't be fetched, or leaves the name
// out if it has none.
func (t texts) attachmentFailed(name string) string {
	return strings.Join(strings.Fields(fmt.Sprintf(t.AttachmentFailed, name)), " ")
}
