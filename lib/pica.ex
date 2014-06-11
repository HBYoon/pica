defmodule Pica do

  alias :erlang,  as: Erlang
  alias :file,    as: F
  alias :filelib, as: Flib
  alias :lists,   as: L
  alias :math,    as: Math
  
  require Record
  
  ## set your own version
  @version     1
  @versionByte 2
  @versionBit  @versionByte * 8
  @idFix       "pica"
  @fixByte     byte_size(@idFix) + @versionByte
  @fixBit      @fixByte * 8
  
  ## /version(2), pica(4), 0(2), 0(24)
  @header      32
  @headerFill  (@header - @fixByte) * 8
  
  ## crc32 + 48Bit position
  @crcByte     4
  @offByte     6
  @offsetByte  @crcByte + @offByte
  @footer      @offsetByte
  
  @crcBit      @crcByte * 8
  @offBit      @offByte * 8
  @offsetBit   @offsetByte * 8
  
  ## by practical reason, limitation of subKeyBit will be sub 20
  @subKeyBit   18
  @keyBit      @subKeyBit * 2
  @maxKey      Erlang.trunc(Math.pow(2, @subKeyBit))
  @maxPk       @maxKey * @maxKey
  
  @offsetTable @offsetByte * @maxKey
  @fullHeader  @offsetTable + @header + @footer
  @fhBit       @fullHeader * 8
  
  Record.defrecordp :pica_rec,
                    [ block: 0,
                      data: 0,
                      bOff: 0,
                      dOff: 0,
                      file: nil ]
  
  
  
  ## macro
  defmacrop return_err(result, whenOk) do
    {_, doBlock} = L.keyfind(:do, 1, whenOk)
    quote do 
      case unquote(result) do
        {:error, reason} -> 
          {:error, reason}
        _ok -> 
          unquote(doBlock)
      end
    end
  end
  
  
  
  def close(pica_rec( file: fd )), 
    do: F.close(fd)
  
  @doc"""
    append data to pica file
  """
  def append( pica_rec( file: fd ) = pica, data) do
    case do_append(pica, data) do
      {:error, reason} -> 
        F.close(fd)
        {:error, reason}
      ok -> ok
    end
  end
  def do_append( pica_rec( block: b, data: d, bOff: bof, dOff: dof, file: fd ) = pica, data) 
  when d < @maxKey and b < @maxKey do
    nextOff = dof + byte_size(data)
    
    dataWriter = {bof + dof, data}
    posWriter  = {calc_offset_pos(bof, d), pack_offset(nextOff)}
    
    return_err F.pwrite(fd, [dataWriter, posWriter]), 
      do: pica_rec(pica, [ data: d+1, dOff: nextOff ]) |> fin_append
  end
  
  def do_append(_pica,_data), 
    do: {:error, :eof}
  
  ## append next block
  defp fin_append(pica_rec( block: b, data: @maxKey, bOff: bof, dOff: dof, file: fd ) = pica) 
  when b < @maxKey do
    nextBlock = b + 1
    nextOffset = bof + dof
    
    blockWriter = {calc_offset_pos(0, b), pack_offset(nextOffset)}
    headerSet = {nextOffset + @fullHeader, [0]}
    
    return_err F.pwrite(fd, [headerSet, blockWriter]), 
      do: {:ok, pica_rec(pica, [ block: nextBlock, 
                                 data: 0, 
                                 bOff: nextOffset, 
                                 dOff: @fullHeader ])}
  end
  
  ## normal or pk overflow
  defp fin_append(pica), 
    do: {:ok, pica}
  
  defp pack_offset(n), 
    do: pack_offset(calc_off_crc(n), n)
  defp pack_offset(crc, n), 
    do: <<crc:: @crcBit, n:: @offBit, 0:: @offsetBit>>
  
  
  @doc"""
    set the pica record to rollback position
    {:ok, pica}
  """
  def past_to(pica_rec( file: fd ), pk) do
    return_err {_, addr} = read_address(fd, {pk, pk}), 
      do: do_past_to(addr, fd, pk)
  end
  
  defp do_past_to([{b, offset, [{d, start}, {_,_fin}]}], fd, pk) do
    ^pk = to_pk(b, d)
    {h, t} = to_subk(pk)
    {:ok, pica_rec( block: h, 
                    data: t, 
                    bOff: offset, 
                    dOff: start, 
                    file: fd )}
  end
  
  defp do_past_to(_any, _fd, _pk), do: {:error, :eof}
  
  
  
  def get_address(pica_rec( file: fd ), pk), 
    do: get_address(fd, pk)
  def get_address(fd, {from, to}) do
    return_err {_, addr} = do_get_address(fd, {from, to}),
      do: {:ok, L.flatten(addr)}
  end
  def get_address(fd, pk), 
    do: get_address(fd, {pk, pk})
  
  defp do_get_address(fd, {from, to}) do
    return_err {_,addr} = read_address(fd, {from, to}), 
      do: {:ok, flatten_addr(addr, [])}
  end
  
  
  
  @doc"""
    {:ok, [data]}
  """
  def get(pica_rec( file: fd ), pk), 
    do: get(fd, pk)
  def get(fd, {from, to}) do
    return_err {_, addr} = do_get_address(fd, {from, to}), 
      do: multi_read(fd, addr)
  end
  def get(fd, pk), 
    do: get(fd, {pk, pk})
  
  defp multi_read(fd, addr) do
    return_err {_, data} = (calc_disk_addr(addr, []) |> read_disk(fd, [])), 
      do: {:ok, L.flatten(data)}
  end
  
  ## {block, bOff, [{data, dOff}]} -> [ [{position, length}] ]
  ## list of position and length will be grouped by the block
  defp flatten_addr([], result), 
    do: L.reverse(result)
  defp flatten_addr([{_blc, offset, dList} | tail], result) do
    faList = start_do_flatten_addr(dList, offset)
    flatten_addr(tail, [faList | result])
  end
  
  defp start_do_flatten_addr([], _off), 
    do: []
  defp start_do_flatten_addr([{_p, off} | tail], offset), 
    do: do_flatten_addr(tail, offset, [{off + offset, nil}])
    
  defp do_flatten_addr([], _off, result), 
    do: L.reverse(result)
  defp do_flatten_addr([{_p, off} | tail], offset, [{dof, nil} | result]) do
    nextOff = off + offset
    do_flatten_addr(tail, offset, [{nextOff, nil}, {dof, nextOff - dof} | result])
  end
  
  ## [ [{position, length}] ] -> [{position, groupLength, [dataLength]}]
  ## for F.pread and bin_split functions
  defp calc_disk_addr([], result), 
    do: L.reverse(result)
  defp calc_disk_addr([faList | tail], result) do
    case start_calc_in_block_addr(faList) do
      nil  -> calc_disk_addr(tail, result)
      addr -> calc_disk_addr(tail, [addr | result])
    end
  end
  
  ## when overflow in existing block
  defp start_calc_in_block_addr([]), do: nil
  ## when requested last + 1 pk
  defp start_calc_in_block_addr([{_po, nil} | []]), do: nil
  defp start_calc_in_block_addr([{pos, len} | tail]) do
    {endPos, lenList} = calc_in_block_addr(tail, [len])
    {{pos, endPos - pos}, lenList}
  end
  
  defp calc_in_block_addr([{pos, nil}|_], lenList), 
    do: {pos, L.reverse(lenList)}
  defp calc_in_block_addr([{_pos, len}|tail], lenList), 
    do: calc_in_block_addr(tail, [len | lenList])
  
  
  defp read_disk([], _fd, result), 
    do: {:ok, L.reverse(result)}
  defp read_disk([{{start, len}, lenList} | tail], fd, result) do
    return_err {_, bin} = F.pread(fd, start, len) do
      dataList = split_bin(lenList, bin, [])
      read_disk(tail, fd, [dataList | result])
    end
  end
  
  defp split_bin([], _, result), 
    do: L.reverse(result)
  defp split_bin([len|next], bin, result) do
    <<data::[binary, size(len)], tail:: binary>> = bin
    split_bin(next, tail, [data|result])
  end
  
  
  
  @doc """
    open a writing descriptor in last state. 
    for read working, use normal erlang file descriptor with raw and binary options.
    {:ok, pica}
  """
  def open(path) do
    append_to = Flib.is_file(path)
    return_err {_, fd} = F.open(path, [:read, :write, :raw, :binary]) do
      case do_open(append_to, fd) do
        {:error, reason} -> 
          F.close(fd)
          {:error, reason}
        ok -> ok
      end
    end
    
  end
  
  defp do_open(true,  fd), do: set_append_file(fd)
  defp do_open(false, fd), do: set_new_file(fd)
  
  defp set_new_file(fd) do
    return_err F.pwrite(fd, [ {0, <<@version:: @versionBit, @idFix, 0:: @fhBit>>}, 
                              {(@fullHeader * 2) - @fixByte, [0]} ]), 
      do: {:ok, pica_rec([ block: 0, 
                           data: 0, 
                           bOff: @fullHeader, 
                           dOff: @fullHeader, 
                           file: fd])}
  end
  
  
  defp set_append_file(fd) do
    return_err {_, bin} = F.pread(fd, 0, @header), 
      do: check_append_file(bin, fd)
  end
  
  defp check_append_file(<<@version:: @versionBit, @idFix, _opts::binary>>, fd) do
    return_err {_, result} = get_last(fd) do
      {{lb, bOff}, {ld, dOff}} = result
      pica_rec( block: lb, 
                data: ld, 
                bOff: bOff, 
                dOff: dOff, 
                file: fd ) |> fin_append
    end
  end
  
  defp check_append_file(_,_),
    do: {:error, :undefined}
  
  
  @doc"""
    {:ok, {{blockKey, blockOffset}, {dataKey, dataOffset}}}
  """
  def get_last(fd) do
    return_err {_, blockList} = read_offset(fd, 0, {0, @maxKey}),
      do: L.last(blockList) |> get_last_data_offset(fd)
  end
  
  defp get_last_data_offset({_, offset} = blc, fd) do
    return_err {_, dataOffList} = read_offset(fd, offset, {0, @maxKey}), 
      do: {:ok, {blc, L.last(dataOffList)}}
  end
  
  
  @doc"""
    read real offset value from disk
    [{blockKey, blockOffset, [{dataKey, dataOffset}]}]
  """
  def read_address(pica_rec( file: fd ), key),
    do: read_address(fd, key)
  def read_address(fd, {from, to}) when from < @maxPk and to < @maxPk and from <= to, 
    do: get_read_range(from, to) |> read_block_and_data_offset(fd)
  def read_address(_, _), 
    do: {:error, :undefined}
  
  
  ## pk ~ ok -> logical address of block and data
  defp get_read_range(from, to), 
    do: calc_read_range( to_subk(from), to_subk(to), [] )
  
  @maxRange @maxKey - 1
  defp calc_read_range({hh, ht}, {hh, tt}, result), 
    do: L.reverse([{hh, ht, tt} | result])
  defp calc_read_range({hh, ht}, {th, tt}, result), 
    do: calc_read_range({hh + 1, 0}, {th, tt}, [{hh, ht, @maxRange} | result])
  
  
  ## get offset of real data
  defp read_block_and_data_offset(addrRange, fd),
    do: read_block_offset(addrRange, fd, [])
  
  defp read_block_offset([], fd, result),
    do: read_data_offset(result, fd, [])
  defp read_block_offset([{block, from, to} | tail], fd, result) do
    case read_offset(fd, 0, {block, block}) do
      {:error, reason} -> 
        {:error, reason}
      {:ok, [{^block, offset}|_]} -> 
        read_block_offset(tail, fd, [{block, offset, {from, to}} | result])
      _other -> 
        read_block_offset([], fd, result)
    end
  end
  
  defp read_data_offset([], _fd, result), 
    do: {:ok, result}
  defp read_data_offset([{block, off, key} | tail], fd, result) do
    return_err {_, dataAddr} = read_offset(fd, off, key),
      do: read_data_offset(tail, fd, [{block, off, dataAddr} | result])
  end
  
  ## read offset from selected offset positioned block
  ## [{key, offset}]
  defp read_offset(fd, offset, {from, _} = key) do
    return_err {_, bin} = read_raw_offset(fd, offset, key), 
      do: {:ok, revise_offset_binary(bin, key) |> check_and_build_offset_list(from, [])}
  end
  
  ## insert 0 offset when requested 0
  defp revise_offset_binary(bin, {0, _}) do 
    crc = Erlang.crc32( <<@fullHeader :: @offBit>> )
    off0 = <<crc:: @crcBit, @fullHeader:: @offBit>>
    <<off0:: binary, bin:: binary>>
  end
  defp revise_offset_binary(bin, _),
    do: bin
  
  ## crc check and list build
  ## every crc checking for reading must checked here.
  defp check_and_build_offset_list(<<crc:: @crcBit, off:: @offBit, tail:: binary>>, start, result) do
    case calc_off_crc(off) do
      ^crc -> check_and_build_offset_list(tail, start + 1, [{start, off} | result])
      _any -> L.reverse(result)
    end
  end
  defp check_and_build_offset_list(_, _, result), 
    do: L.reverse(result)
  
  defp read_raw_offset(fd, offset, {0, to}),
    do: do_read_raw_offset(fd, offset, {0, to})
  defp read_raw_offset(fd, offset, {from, to}),
    do: do_read_raw_offset(fd, offset, {from - 1, to})
    
  defp do_read_raw_offset(fd, offset, {from, to}) do
    start = calc_offset_pos(offset, from)
    fin = calc_offset_pos(offset, to)
    F.pread(fd, start, fin - start + @offsetByte) 
  end
  
  
  @doc"""
    get last inserted pk
  """
  def last_pk(pica), 
    do: current_pk(pica) - 1
    
  @doc"""
    get current inserting pk
  """
  def current_pk(pica_rec( block: b, data: d )), 
    do: to_pk(b, d)
  
  defp to_pk(h, t), do: 
    to_pk(<<h:: @subKeyBit, t:: @subKeyBit>>)
  defp to_pk(<<pk:: @keyBit>>), 
    do: pk
  
  defp to_subk(pk),
    do: do_to_subk(<<pk:: @keyBit>>)
  defp do_to_subk(<<h:: @subKeyBit, t:: @subKeyBit>>),
    do: {h, t}
  
  defp calc_off_crc(n), 
    do: calc_crc(<<n:: @offBit>>)
  defp calc_crc(data),
    do: Erlang.crc32(data)
  
  defp calc_offset_pos(offset, count), 
    do: offset + @header + (count * @offsetByte) 
  
end


"""
defmodule Pica.Test do

  def start(name, n) do
    {:ok, pica} = Pica.open name
    start = Pica.current_pk(pica)
    
    :io.format 'test start from ~p ~n', [start]
    
    {t1, {:ok, pica}} = :timer.tc fn() ->
      do_loop_append_test(pica, name, :crypto.strong_rand_bytes(2048), start, n, 0)
    end
    :io.format 'append loop end>> ~p~n', [t1]
    
    {t2, _} = :timer.tc fn() ->
      do_check_test(pica, start, 0, n)
    end
    :io.format 'get loop end>> ~p~n', [t2]
    
    {t3, result} = :timer.tc fn() ->
      m_read_test(pica, name, start, 0)
    end
    :io.format 'm_get loop end>> ~p~n', [{t3, result}]
    
    {:ok, pica}
  end
  
  def do_loop_append_test(pica,_name,_bin,_start, 0,_v), do: {:ok, pica}
  def do_loop_append_test(pica, name, bin, start, n, v) do
    data = {v, :binary.part( bin, 0, :crypto.rand_uniform(1, 1024) )}
    dPack = Erlang.term_to_binary(data)
    crc = Erlang.crc32 dPack
    {:ok, pica} = Pica.append pica, <<crc::32, dPack::binary>>
    if rem(n, 4500) == 0 do
      Pica.close(pica)
      {:ok, pica} = Pica.open(name)
      
      do_loop_append_test(pica, name, bin, start, n-1, v+1)
    else
      do_loop_append_test(pica, name, bin, start, n-1, v+1)
    end
  end
  
  def do_check_test(_p, _s, v, v), do: :ok
  def do_check_test(pica, start, v, vv) do
  
    {:ok, [b]} = Pica.get pica, (start + v)
    
    vector = v
    
    {^vector, _bin} = d_check(b)
    do_check_test(pica, start, v+1, vv)
  end
  
  
  def m_read_test(p, n, s, v, c\\0) do
    if (s+v+1001) < @maxPk do
      case Pica.get(p, {s+v, s+v+500}) do
        {:ok, []} -> {:ok, v - 1}
        {:ok, list} -> 
          v = check_m_read_test(list, v)
          
          if rem(c, 500) == 0 do
            Pica.close(p)
            {:ok, p} = Pica.open(n)
            
            m_read_test(p, n, s, v, c+1)
          else
            m_read_test(p, n, s, v, c+1)
          end
      end
    else
      {:ok, list} = Pica.get(p, {s+v, @maxPk - 1})
      check_m_read_test(list, v) - 1
    end
  end
  
  def check_m_read_test([], v), do: v
  def check_m_read_test([b|tail], v) do
    vector = v
    {^vector, _bin} = d_check(b)
    check_m_read_test(tail, v+1)
  end
  
  defp d_check(<<crc::32, d::binary>>) do
    ^crc = Erlang.crc32(d)
    Erlang.binary_to_term(d)
  end

end
"""