#!/usr/bin/env ruby
# -*- coding:utf-8 -*-

require 'time'
require 'csv'

InputDataPath  = './patients_summary.csv'
OutputDataPath = './hokkaido-covid19-poscases.csv'

Sources = {
  :for_hokkaido =>
  'https://www.harp.lg.jp/opendata/dataset/1369.html'
}

StartDate = Time.local(2020, 1, 22);

$data = CSV.read(InputDataPath, encoding: 'sjis')
$hash = Hash.new
time = nil
count = 0
(1 .. $data.size - 1).each do |index|
  time = Time.parse($data[index][0])
  new_cases = $data[index][1].to_i
  count += new_cases
  $hash[time] = count
end
end_time = time
$array = Array.new
date = StartDate
while (date <= end_time) do
  if $hash.has_key?(date)
    $array.push($hash[date])
  else
    $array.push(0)
  end
  date += 24 * 60 * 60
end
File.open(OutputDataPath, 'w') do |out|
  out.print(',"Hokkaido",0.0,0.0, ')
  out.puts($array.join(', '))
end

##
