#!/usr/bin/env ruby
# -*- coding:utf-8 -*-

require 'csv'
require 'date'
require 'json'

PrefecturesPath   = './prefectures.csv'
PopulationPath    = './population_prefs.csv'
JPCovid19DataPath = './Jag/COVID-19.csv'

JSONDataPath      = './jp_covid19_confirmed.json'

Sources = {
  :for_data =>
  'JAG Japan https://gis.jag-japan.com/covid19jp/',
  :for_population =>
  '統計センター https://www.e-stat.go.jp/dbview?sid=0003312315',
}

#-------

class CSVDB

  attr_reader :indices
  attr_reader :rows
  
  class Row

    attr_reader :indices
    attr_reader :array

    def initialize(owner_, array_)
      @indices = owner_.indices
      @array   = array_
    end

    def ref(label_)
      return @array[@indices[label_]]
    end
    def [](label_)
      return ref(label_)
    end

  end

  def new_row(array_)
    return Row.new(self, array_)
  end

  def append(row_array_)
    row = new_row(row_array_)
    @rows.push(row)
    return row
  end
  
  def initialize(path_)
    data = CSV.read(path_, skip_lines: /^#/)
    labels = data[0]
    @indices = Hash.new
    labels.each_index do |index|
      @indices[labels[index]] = index
    end
    @rows = Array.new
    data.each_index do |index|
      next if index.zero?
      append(data[index])
    end
  end
    
end

#-------

class Prefectures < CSVDB

  # :indices
  # :rows
  
  class Row < CSVDB::Row

    # :indices
    # :array

    def ja_name; return ref('Name_ja')      end
    def en_name; return ref('Name_en')      end
    def iso_no;  return ref('ISO/JIS').to_i end
    def region;  return ref('地方')         end

  end

  attr_reader :by_ja_name
  attr_reader :by_en_name
  attr_reader :by_iso_no

  def new_row(array_)
    row =Prefectures::Row.new(self, array_)
    @by_ja_name[row.ja_name] = row
    @by_en_name[row.en_name] = row
    @by_iso_no[row.iso_no]   = row
    return row
  end

  def initialize(path_)
    @by_ja_name = Hash.new
    @by_en_name = Hash.new
    @by_iso_no  = Hash.new
    super(path_)
  end

end

$prefectures = Prefectures.new(PrefecturesPath)

#-------

class Population < CSVDB

  # :indices
  # :rows
  
  class Row < CSVDB::Row

    # :indices
    # :array

    def iso_no;     return ref('都道府県コード').to_i end
    def ja_name;    return ref('都道府県名')          end
    def population; return 1000.0 * ref('人口').to_f  end

  end

  attr_reader :by_ja_name
  attr_reader :by_iso_no

  def new_row(array_)
    row = Population::Row.new(self, array_)
    @by_ja_name[row.ja_name] = row
    @by_iso_no[row.iso_no]   = row
    return row
  end

  def initialize(path_)
    @by_ja_name = Hash.new
    @by_iso_no  = Hash.new
    super(path_)
  end

end

$population = Population.new(PopulationPath)

#-------

class JPCovid19Data < CSVDB

  # :indices
  # :rows
  
  class Row < CSVDB::Row

    # :indices
    # :array
    
    def mdy_to_iso(date_mdy_)
      if /^\s*(\d+)\s*\/\s*(\d+)\s*\/\s*(\d+)\s*$/ =~ date_mdy_
        return sprintf('%04d-%02d-%02d', $3, $1, $2)
      else
        return nil
      end
    end

    def date;    return mdy_to_iso(ref('確定日'))      end
    def pref_id; return ref('居住都道府県コード').to_i end

  end

  DATE_MAX = '9999-12-31'
  DATE_MIN = '0000-01-01'
  
  attr_reader :date_first
  attr_reader :date_last

  def new_row(array_)
    row = JPCovid19Data::Row.new(self, array_)
    @date_first = row.date if row.date < @date_first
    @date_last  = row.date if @date_last < row.date
    return row
  end

  def initialize(path_)
    @date_first = DATE_MAX
    @date_last  = DATE_MIN
    super(path_)
  end

end

$covid19data = JPCovid19Data.new(JPCovid19DataPath)

#-------

def get_ja_name(ja_name_)
  ja_name = nil
  if    /^(.*)都$/ =~ ja_name_
    ja_name = $1
  elsif /^(.*道)$/ =~ ja_name_
    ja_name = $1
  elsif /^(.*)府$/ =~ ja_name_
    ja_name = $1
  elsif /^(.*)県$/ =~ ja_name_
    ja_name = $1
  end
  return ja_name
end

LocalRegionTable = {
  '北海道' => 'hokkaido',
  '東北' => 'tohoku',
  '関東' => 'kanto',
  '中部' => 'chubu',
  '近畿' => 'kinki',
  '中国' => 'chugoku',
  '四国' => 'shikoku',
  '九州' => 'kyushu',
  '沖縄' => 'kyushu',
}

def get_region(region_)
  region = nil
  if LocalRegionTable.has_key?(region_)
    region = LocalRegionTable[region_]
  end
  return region
end

StartDate  = Date.parse('2020-01-15')
$date_last = Date.parse($covid19data.date_last)

$table_pd = Hash.new

$covid19data.rows.each do |row|
  date    = row.date
  pref_id = row.pref_id
  if $table_pd.has_key?(pref_id)
    if $table_pd[pref_id].has_key?(date)
      $table_pd[pref_id][date] += 1
    else
      $table_pd[pref_id][date] = 1
    end
  else
    $table_pd[pref_id] = Hash.new
    $table_pd[pref_id][date] = 1
  end
end

$table_pc = Hash.new
$prefectures.by_iso_no.keys.sort.each do |pref_id|
  entry = Hash.new
  entry['iso_numeric'] = pref_id
  entry['iso_alpha2']  = sprintf('JP-%02d', pref_id)
  entry['name_en']     = $prefectures.by_iso_no[pref_id].en_name
  entry['name_ja']     = get_ja_name($prefectures.by_iso_no[pref_id].ja_name)
  entry['region']      = get_region($prefectures.by_iso_no[pref_id].region)
  entry['population']  = $population.by_iso_no[pref_id].population
  pc_date_array = Array.new
  if $table_pd.has_key?(pref_id)
    pd_date_counts = $table_pd[pref_id]
    count = 0
    date = StartDate
    while date <= $date_last do
      if pd_date_counts.has_key?(date.iso8601)
        count += pd_date_counts[date.iso8601]
      end
      pc_date_array.push(count)
      date += 1
    end
  else
    pc_date_array = Array.new
    date = StartDate
    while date <= $date_last do
      pc_date_array.push(0)
      date += 1
    end
  end
  entry['num_cases'] = pc_date_array
  $table_pc[entry['iso_alpha2']] = entry
end
File.open(JSONDataPath, 'w') do |out|
  out.puts(JSON.generate($table_pc))
end

##
