--Selection with GIT-LOC entities only
WITH data_text_LOC AS (SELECT
  id_data_text AS id
FROM
  git430_ner.entity
WHERE
	id_ent_type = 16)
SELECT
  git400_token.sentence.id_data_text,
  git400_token.sentence.id,
  git300_scrap.data_text.content AS post,
  git400_token.sentence.txt AS sentence,
  git400_token.word.txt AS word,
  git400_token.word.txt_lemm AS lemma,
  git400_token.word.id_entity,
  git430_ner.entity.id_ent_type,
  git430_ner.ent_type.name,
  git400_token.sentence.id_project,
  git400_token.sentence.id_www_sources,
  git400_token.sentence.order_num AS ord_sent,
  git400_token.word.order_num AS ord_word
FROM
  git400_token.word
  LEFT OUTER JOIN git400_token.sentence ON (git400_token.word.id_sentence = git400_token.sentence.id)
  LEFT OUTER JOIN git430_ner.entity ON (git400_token.word.id_entity = git430_ner.entity.id)
  LEFT OUTER JOIN git430_ner.ent_type ON (git430_ner.entity.id_ent_type = git430_ner.ent_type.id)
  LEFT OUTER JOIN git300_scrap.data_text ON (git300_scrap.data_text.id = git400_token.word.id_data_text)
  INNER JOIN data_text_LOC ON (data_text_LOC.id = git400_token.word.id_data_text)
WHERE
  (True OR
  git400_token.sentence.id = 102107) AND
  git400_token.sentence.id_data_text >= 22443679 AND
  git400_token.sentence.id_data_text <= 23622210
ORDER BY
  git400_token.word.id_data_text,
  git400_token.word.id_sentence,
  git400_token.sentence.order_num,
  git400_token.word.order_num


--Selection limited by id_data_text
SELECT
  git400_token.sentence.id_data_text,
  git400_token.sentence.id,
  git300_scrap.data_text.content AS post,
  git400_token.sentence.txt AS sentence,
  git400_token.word.txt AS word,
  git400_token.word.txt_lemm AS lemma,
  git400_token.word.id_entity,
  git430_ner.entity.id_ent_type,
  git430_ner.ent_type.name,
  git400_token.sentence.id_project,
  git400_token.sentence.id_www_sources,
  git400_token.sentence.order_num AS ord_sent,
  git400_token.word.order_num AS ord_word
FROM
  git400_token.word
  LEFT OUTER JOIN git400_token.sentence ON (git400_token.word.id_sentence = git400_token.sentence.id)
  LEFT OUTER JOIN git430_ner.entity ON (git400_token.word.id_entity = git430_ner.entity.id)
  LEFT OUTER JOIN git430_ner.ent_type ON (git430_ner.entity.id_ent_type = git430_ner.ent_type.id)
  LEFT OUTER JOIN git300_scrap.data_text ON (git300_scrap.data_text.id = git400_token.word.id_data_text)
WHERE
  (True OR
  git400_token.sentence.id = 102107) AND
  git400_token.sentence.id_data_text >= 22443679 AND
  git400_token.sentence.id_data_text <= 23622210
ORDER BY
  git400_token.word.id_data_text,
  git400_token.word.id_sentence,
  git400_token.sentence.order_num,
  git400_token.word.order_num