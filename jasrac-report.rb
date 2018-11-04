require "google/cloud/bigquery"
require 'net/http'
require 'uri'

if ARGV.size() < 2 then
 print "Usage: script.rb year month\n"
 return
end

master = JSON.parse(Net::HTTP.get URI.parse('https://song-book.info/v3/prod-master-detail.json'))
songs = master["songs"]

bigquery = Google::Cloud::Bigquery.new(
  project: "songbook-7c92d", #BigQueryのプロジェクトID
  keyfile: "./auth.json" #認証用JSONキーファイル
)
year = ARGV[0]
month = ARGV[1]

beginDate = Date.new(year.to_i, month.to_i, 1)
endDate   = beginDate.next_month.prev_day(1)

sql = "SELECT event_params.value.int_value AS song_id, COUNT(*) AS count
FROM TABLE_DATE_RANGE( [analytics_166204476.events_], TIMESTAMP('#{beginDate.strftime("%Y-%m-%d")}'), TIMESTAMP('#{endDate.strftime("%Y-%m-%d")}'))
WHERE event_params.key = 'song_id' AND event_params.value.int_value IS NOT NULL
GROUP BY song_id
LIMIT 1000"

data = bigquery.query sql, legacy_sql: true
data.each do |row|
  song_id = row[:song_id]
  count = row[:count]
  song = songs.find{|song| song["id"] == song_id }
  if song != nil then
    result = Array.new

    # インターフェースコード
    result.push song["id"]
    # コンテンツ区分
    result.push nil
    # コンテンツ枝番
    result.push 0
    # メドレー区分
    result.push nil
    # メドレー枝番
    result.push nil
    # コレクトコード
    result.push nil
    # jasracコード
    result.push song["jasrac_code"]
    # タイトル
    result.push song["title"]
    # 副題
    result.push nil
    # 作詞者名
    result.push song["lyricist"]
    # 訳
    result.push nil
    # 作曲者名
    result.push song["composer"]
    # 編曲者名
    result.push nil
    # アーティスト名
    result.push nil
    # 情報量
    result.push 0
    # IVT区分
    result.push song["show_words"] ? "V" : "I"
    # 原詩訳詞区分
    result.push song["show_words"] ? 3 : nil
    # IL区分
    result.push nil
    # リクエスト回数
    result.push count.to_i

    puts result.join("\t").encode("Shift_JIS", :invalid => :replace, :undef => :replace)
  end

end
