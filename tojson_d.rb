#!/usr/bin/env ruby
# -*- coding:utf-8 -*-

require 'csv'
require 'json'

CountriesPath    = './iso3166-1.csv'
PopulationPath   = './population.csv'
Covid19CDataPath = './COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
Covid19DDataPath = './COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
HokkaidoDataPath = './hokkaido-covid19-poscases.csv'

JSONDataPath     = './csse_covid19_deaths.json'

Sources = {
  :for_data =>
  'CSSE, Johns Hopkins Univ. https://github.com/CSSEGISandData/COVID-19',
  :for_population =>
  'World Population Prospects 2019, UN. https://population.un.org/wpp/Download/Standard/Population',
  :for_population_additonal =>
  'https://en.wikipedia.org/wiki/Hubei',
  :for_hokkaido =>
  '北海道オープンデータポータル, 北海道. https://www.harp.lg.jp/opendata/dataset/1369.html'
}

#-------

class CSVDB

  attr_reader :indices
  attr_reader :rows
  
  class Row

    attr_reader :owner
    attr_reader :array

    def initialize(owner_, array_)
      @owner = owner_
      @array = array_
    end

    def ref(label_)
      return @array[owner.indices[label_]]
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
    array = CSV.read(path_, skip_lines: /^#/)
    labels = array[0]
    @indices = Hash.new
    labels.each_index do |index|
      @indices[labels[index]] = index
    end
    @rows = Array.new
    array.each_index do |index|
      next if index.zero?
      append(array[index])
    end
  end
    
end

#-------

class Countries < CSVDB

  class Row < CSVDB::Row

    def ja_name;     return ref('国・地域名')   end
    def en_name;     return ref('英語名')       end
    def iso_numeric; return ref('numeric').to_i end
    def iso_code3;   return ref('alpha-3')      end
    def iso_code2;   return ref('alpha-2')      end
    def region;      return ref('大区分')       end

  end

  def new_row(array_)
    return Row.new(self, array_)
  end

  attr_reader :indices_en_name
  attr_reader :indices_iso_numeric

  def append(row_array_)
    row = super(row_array_)
    @indices_en_name[row.en_name] = row
    @indices_iso_numeric[row.iso_numeric] = row
  end

  def initialize(path_)
    @indices_en_name     = Hash.new
    @indices_iso_numeric = Hash.new
    super(path_)
  end

end

$countries = Countries.new(CountriesPath)

HokkaidoCountry = [ '北海道', 'Hokkaido', '1000001', 'JPN-01', 'JP-01', '東アジア', 'ISO 3166-2JP' ]
$countries.append(HokkaidoCountry)

#-------

class Population < CSVDB

  class Row < CSVDB::Row

    def iso_numeric; return ref('LocID').to_i             end
    def en_name;     return ref('Location')               end
    def population;  return 1000.0 * ref('PopTotal').to_f end

  end

  def new_row(array_)
    return Row.new(self, array_)
  end

  attr_reader :indices_en_name
  attr_reader :indices_iso_numeric

  def append(row_array_)
    row = super(row_array_)
    @indices_en_name[row.en_name] = row
    @indices_iso_numeric[row.iso_numeric] = row
  end

  def initialize(path_)
    @indices_en_name     = Hash.new
    @indices_iso_numeric = Hash.new
    super(path_)
  end

end

$population = Population.new(PopulationPath)

HokkaidoPopulation = [ '1000001', 'Hokkaido', '2', 'Medium', '2020', '2020.0', '2467.664', '2754.437', '5264.193', '0.0' ]
$population.append(HokkaidoPopulation)

#-------

class Covid19Data < CSVDB

  class Row < CSVDB::Row

    FirstDataCol = 4

    attr_reader :data

    def province;    return ref('Province/State') end
    def en_name;     return ref('Country/Region') end

    def initialize(owner_, array_)
      super(owner_, array_)
      @array.pop if @array.last.nil?
      @data = Array.new
      (FirstDataCol .. @array.size - 1).each do |index|
        @data.push(@array[index].to_i)
      end
    end

  end

  def new_row(array_)
    return Row.new(self, array_)
  end

  def initialize(path_)
    super(path_)
  end

end

$covid19data = Covid19Data.new(Covid19DDataPath)
#hokkaido_data = CSV.read(HokkaidoDataPath)
#$covid19data.append(hokkaido_data[0])

#-------

class Table

  class Entry

    attr_reader :country_id
    attr_reader :num_cases
    attr_reader :per_capita
    attr_reader :cross_index

    def initialize(country_id_, num_cases_)
      @country_id  = country_id_
      @num_cases   = num_cases_
      @per_capita  = Array.new
      @cross_index = nil
    end

    def add(num_cases_)
      @num_cases.each_index do |index|
        unless num_cases_[index].nil?
          @num_cases[index] += num_cases_[index]
        else
          @num_cases[index] += 0
        end
      end
    end

    def calc_per_capita(population_)
      @num_cases.each_index do |index|
        @per_capita[index] = @num_cases[index].to_f / population_
      end
    end
    
    def set_cross_index(cross_index_)
      @cross_index = cross_index_
    end

  end

  attr_reader :entries
  
  def initialize
    @entries = Hash.new
  end

  def add(country_id_, covid19data_row_)
    unless @entries.has_key?(country_id_)
      @entries[country_id_] = Entry.new(country_id_, covid19data_row_.data)
    else
      @entries[country_id_].add(covid19data_row_.data)
    end
  end

  def calc_per_capita(country_id_, population_)
    @entries[country_id_].calc_per_capita(population_)
  end

  def set(entry_)
    @entries[entry_.country_id] = entry_
  end

end

#-------

warn("all data have loaded")

LocalEnNameTable = {
  'Bahamas, The' => 'Bahamas',
  'Bolivia' => 'Bolivia, Plurinational State of',
  'Brunei' => 'Brunei Darussalam',
  'Cabo Verde' => 'Cape Verde',
  'Congo (Brazzaville)' => 'Congo',
  'Congo (Kinshasa)' => 'Congo, the Democratic Republic of the',
  'Cote d\'Ivoire' => 'Côte d\'Ivoire',
  'Czech Republic' => 'Czechia',
  'East Timor' => 'Timor-Leste',
  'Gambia, The' => 'Gambia',
  'Holy See' => 'Holy See (Vatican City State)',
  'Hong Kong SAR' => 'Hong Kong',
  'Iran' => 'Iran, Islamic Republic of',
  'Iran (Islamic Republic of)' => 'Iran, Islamic Republic of',
  'Laos' => 'Lao People\'s Democratic Republic',
  'Macao SAR' => 'Macau',
  'Mainland China' => 'China',
  'Moldova' => 'Moldova, Republic of',
  'Korea, South' => 'Korea, Republic of',
  'occupied Palestinian territory' => 'Palestinian Territory, Occupied',
  'Republic of Korea' => 'Korea, Republic of',
  'Republic of Moldova' => 'Moldova, Republic of',
  'Reunion' => 'Réunion',
  'Russia' => 'Russian Federation',
  'Saint Barthelemy' => 'Saint Barthélemy',
  'Syria' => 'Syrian Arab Republic',
  'Taipei and environs' => 'Taiwan, Province of China',
  'Taiwan*' => 'Taiwan, Province of China',
  'Tanzania' => 'Tanzania, United Republic of',
  'The Bahamas' => 'Bahamas',
  'The Gambia' => 'Gambia',
  'UK' => 'United Kingdom',
  'US' => 'United States',
  'Venezuela' => "Venezuela, Bolivarian Republic of",
  'Vietnam' => 'Viet Nam',
}

LocalJaNameTable = {
  'アメリカ合衆国' => 'アメリカ',
  '大韓民国' => '韓国',
  '中華人民共和国' => '中国 (大陸)',
  '中国台湾省' => '台湾',
  'ロシア連邦' => 'ロシア',
}

LocalRegionTable = {
  '東アジア' => 'asia',
  '東南アジア' => 'asia',
  '中央アジア' => 'asia',
  '南アジア' => 'asia',
  '中東' => 'asia',
  '北ヨーロッパ' => 'europe',
  '東ヨーロッパ' => 'europe',
  '西ヨーロッパ' => 'europe',
  'ロシア' => 'europe',
  '地中海地域' => 'europe',
  '北アフリカ' => 'africa',
  '東アフリカ' => 'africa',
  '中央アフリカ' => 'africa',
  '西アフリカ' => 'africa',
  '南アフリカ' => 'africa',
  '北アメリカ' => 'america',
  '中央アメリカ' => 'america',
  '南アメリカ' => 'america',
  'オセアニア' => 'oceania',
}

LocalRegionCTable = {
  'British Indian Ocean Territory' => 'asia',
  'Comoros' => 'africa',
  'Seychelles' => 'africa',
  'Madagascar' => 'africa',
  'Mayotte' => 'africa',
  'Maldives' => 'asia',
  'Réunion' => 'africa',
}

def get_continent(en_name_, region_)
  continent = nil
  if    LocalRegionTable.has_key?(region_)
    continent = LocalRegionTable[region_]
  elsif LocalRegionCTable.has_key?(en_name_)
    continent = LocalRegionCTable[en_name_]
  end
  return continent
end

LocalStyleTypeTable = {
  'Japan'                     =>  2,
  'Korea, Republic of'        =>  3,
  'Hong Kong'                 =>  4,
  'Singapore'                 =>  5,
  'France'                    =>  6,
  'Malaysia'                  =>  7,
  'Germany'                   =>  8,
  'Italy'                     =>  9,
  'United Kingdom'            => 10,
  'Sweden'                    => 11,
  'Spain'                     => 12,
  'Belgium'                   => 13,
  'Iran, Islamic Republic of' => 14,
  'Bahrain'                   => 15,
  'Kuwait'                    => 16,
  'Switzerland'               => 17,
  'Austria'                   => 18,
  'Norway'                    => 19,
  'Netherlands'               => 20,
  'China'                     => 21,
  'United States'             => 22,
  'Hokkaido'                  => 23,
}
StartStyleType = 24

LatencyDays = 4
Threshould_PerCapita   = 100.0 / 100000000.0
Threshould_NumCases    = 100
Threshould_Population  =  1000000
Threshould_DPopulation = 10000000

# sum up
$table_all = Table.new
$covid19data.rows.each do |row|
  country_name = row.en_name
  if country_name == 'China'
    case row.province
    when 'Hong Kong'
      country_name = 'Hong Kong'
    when 'Macau'
      country_name = 'Macau'
    when 'Taiwan'
      country_name = 'Taiwan'
    end
  end
  if LocalEnNameTable.has_key?(country_name)
    country_name = LocalEnNameTable[country_name]
  end
  if $countries.indices_en_name.has_key?(country_name)
    country_id = $countries.indices_en_name[country_name].iso_numeric
    $table_all.add(country_id, row)
  else
    warn("country/region '#{country_name}' was ignored")
  end
end
warn("#{$table_all.entries.size} countries/regions data have been summed up")
=begin
# divide by population
$table_all.entries.keys.each do |country_id|
  if $population.indices_iso_numeric.has_key?(country_id)
    population = $population.indices_iso_numeric[country_id].population
    $table_all.calc_per_capita(country_id, population)
  else
    warn("population of country id '#{country_id}' is unknown")
  end
end
warn("data have been divided by population")
=end
# pick up
$table_picked = Table.new
$table_all.entries.keys.each do |country_id|
  entry = $table_all.entries[country_id]
  if $population.indices_iso_numeric.has_key?(country_id)
    population = $population.indices_iso_numeric[country_id].population
    if population >= Threshould_Population
      $table_picked.set(entry)
    end
  end
end
warn("#{$table_picked.entries.size} countries have been picked up")
# output
data = Hash.new
$table_picked.entries.keys.sort.each do |country_id|
  entry = $table_picked.entries[country_id]
  country = $countries.indices_iso_numeric[country_id]
  population = $population.indices_iso_numeric[country_id]
  ja_name = country.ja_name
  ja_name = LocalJaNameTable[ja_name] if LocalJaNameTable.has_key?(ja_name)
  data_country = Hash.new
  data_country['name_en']     = country.en_name
  data_country['name_ja']     = ja_name
  data_country['iso_numeric'] = country.iso_numeric
  data_country['iso_alpha2']  = country.iso_code2
  data_country['continent']   = get_continent(country.en_name, country.region)
  data_country['population']  = population.population
  data_country['num_cases']   = entry.num_cases
#  data_country['per_capita']  = entry.per_capita.map!{|value| (1e8 * value).round.to_i }
  data[country.iso_code2] = data_country
end
File.open(JSONDataPath, 'w') do |out|
  out.puts(JSON.generate(data))
end

##
