# include "precompiled_headers.hpp"

// =====================================================================================================================

void worker_t::client_responder ()
{
   while (1) {
      if (m_stopped) break;
      if (! m_client_response_queue.ready ().wait_for (50 MILLISECONDS))
         if (m_client_response_queue->empty ())
            continue;
      client_request_t::ptr req { m_client_response_queue.pop () };
      if (! req) continue;
      json_t & request { req->request }, result, param;
      str id, method;

      for (auto & iter : request) {
         const str & nm { iter.first };
         if (0);
         else if (nm == "id") { id = iter.second.get_value <str> (); }
         else if (nm == "method") { method = iter.second.get_value <str> (); }
         else if (nm == "param") { param = iter.second; }
      }

      str error;
      try {
         if (0);
         else if (boost::iequals (method, "NewTrx"))              json_rpc_new_trx (param, result);
         else if (boost::iequals (method, "GetTrxInfo"))          json_rpc_get_trx_info (param, result);
         else if (boost::iequals (method, "GetLeader"))           json_rpc_get_leader (param, result);
         else if (boost::iequals (method, "GetNodeList"))         json_rpc_get_nodelist (param, result);
         else if (boost::iequals (method, "GetWalletState"))      json_rpc_get_wallet_state (param, result);
         else if (boost::iequals (method, "GetWalletTrxs"))       json_rpc_get_wallet_trxs (param, result);
         else if (boost::iequals (method, "GetBlock"))            json_rpc_get_block (param, result);
         else if (boost::iequals (method, "NftCreate"))           json_rpc_nft_create (param, result);
         else if (boost::iequals (method, "NftAuction"))          json_rpc_nft_auction (param, result);
         else if (boost::iequals (method, "NftBid"))              json_rpc_nft_bid (param, result);
         else if (boost::iequals (method, "NftTransfer"))         json_rpc_nft_transfer (param, result);
         else if (boost::iequals (method, "NftAbort"))            json_rpc_nft_abort (param, result);
         else if (boost::iequals (method, "NftBurn"))             json_rpc_nft_burn (param, result);
         else if (boost::iequals (method, "NftDonate"))           json_rpc_nft_donate (param, result);
         else if (boost::iequals (method, "NftGetColl"))          json_rpc_get_coll_info (param, result);
         else if (boost::iequals (method, "NftGetToken"))         json_rpc_get_nft_info (param, result);
         else if (boost::iequals (method, "NftGetAuctionList"))   json_rpc_get_auction_list (param, result);
         else if (boost::iequals (method, "NftGetAuction"))       json_rpc_get_auction_info (param, result);
         else if (boost::iequals (method, "FtEmission"))          json_rpc_fft_emission (param, result);
         else if (boost::iequals (method, "FtTransfer"))          json_rpc_fft_transfer (param, result);
         else if (boost::iequals (method, "FtBurn"))              json_rpc_fft_burn (param, result);
         else if (boost::iequals (method, "FtGetTokenList"))      json_rpc_get_fft_list (param, result);
         else if (boost::iequals (method, "FtGetToken"))          json_rpc_get_fft_info (param, result);
         else throw "unknown method";
      } catch (const char * msg) {
         error = msg;
      } catch (const str & msg) {
         error = msg;
      } catch (...) {
         error = "internal error";
      }

      req->response.put ("jsonrpc", "2.0");
      if (error.empty ()) {
         req->response.put ("status", "ok");
         req->response.put ("id", id);
         req->response.add_child ("result", result);
      } else {
         req->response.put ("status", "error");
         req->response.put ("id", id);
         req->response.put ("reason", error);
      }
      req->completed = timer::now ();
   }
}

// =====================================================================================================================

void worker_t::client_response_sent (const str & id_)
{
   client_request_t::ptr req { m_client_response_map.get (id_) };
   if (req) req->sent = timer::now ();
   m_client_response_map.remove_if_done (id_);
}

// =====================================================================================================================

buf worker_t::process_CLIENT_REQUEST (json_t & request_, str & client_id_)
{
   if (m_rank_voting.cont ().empty ()) {
      return buf ("HTTP/1.1 503 Not ready yet\r\n\r\n");
   }

   json_t response;
   str id, method;
   for (auto & iter : request_) {
      const str & nm { iter.first };
      if (0);
      else if (nm == "id") { id = iter.second.get_value <str> (); }
      else if (nm == "method") { method = iter.second.get_value <str> (); }
   }

   if (id.empty ()) {
      response.put ("jsonrpc", "2.0");
      response.put ("status", "error");
      response.put ("reason", "request malformed: id is required");
   } else if (method.empty ()) {
      response.put ("jsonrpc", "2.0");
      response.put ("status", "error");
      response.put ("reason", "request malformed: method is required");
   } else {
      client_id_ = id;
      client_request_t::ptr req { m_client_response_map.get (id) };
      if (req) {
         if (req->completed) response = req->response;
      } else {
         req.reset (new client_request_t { request_ });
         m_client_response_map.append (client_id_, req);
         m_client_response_queue.push (req);
         timer::sleep (500 MILLISECONDS);
         if (req->completed) response = req->response;
      }
   }
   
   buf ret, content;
   if (! response.empty ()) {
      std::stringstream ss;
      write_json (ss, response, true);
      content.append (ss.str ());
   }

   if (content.empty ()) {
      ret.append ("HTTP/1.1 100 Not ready yet\r\n\r\n");
   } else {
      ret.append ("HTTP/1.1 200 OK\r\nContent-Length: ")
         .append (std::to_string (content.size ()))
         .append ("\r\n\r\n")
         .append (content);
   }
   return ret;
}

// =====================================================================================================================

void worker_t::json_rpc_new_trx (json_t & params_, json_t & result_)
{
   if (params_.empty ()) throw "missed param";

   vector <remit_t> message_list;

   for (auto & node : params_) {
      if (node.first == "messages") for (auto & node : node.second) {
         remit_t remit;
         for (auto & msg : node.second) {
            str nm { msg.first };
            str vl { msg.second.get_value <str> () };
            if (0) {
            } else if (nm == "type") {
               if (0) {
               } else if (vl == "fee") {
                  remit.type = RMT_FEE;
               } else if (vl == "transfer") {
                  remit.type = RMT_TRANSFER;
               } else if (vl == "delegate") {
                  remit.type = RMT_DELEGATE;
               } else if (vl == "withdraw") {
                  remit.type = RMT_WITHDRAW;
               }
            } else if (nm == "time") {
               try { remit.time = boost::lexical_cast <qword> (vl); } catch (...) {}
            } else if (nm == "payer") {
               remit.src_wallet.from_b58 (vl);
            } else if (nm == "receiver") {
               remit.dst_wallet.from_b58 (vl);
            } else if (nm == "sum") {
               try { remit.sum = boost::lexical_cast <qword> (vl); } catch (...) {}
            } else if (nm == "comment") {
               if (vl.length () > 255) vl.resize (255);
               remit.comment = vl;
            } else if (nm == "sign") {
               remit.sign.from_b58 (vl);
            }
         }
         if (remit.type == RMT_FEE && remit.dst_wallet != m_wallet_oven) {
            str s { "wrong request: fee must be sent to wallet " };
            s += WALLET_OVEN;
            throw s;
         }
         if (remit.time < timer::now () - 5 MINUTES || remit.time > timer::now () + 30 SECONDS)
            throw "invalid time (too old or in future)";
         if (remit.src_wallet == remit.dst_wallet) throw "meaningless transaction";
         if (TRX_MSG_UNDEFINED == remit.type) throw "invalid message type";
         if (0 == remit.sum) throw "invalid amount";
         if (remit.src_wallet.size () != HASH_SIZE) throw "invalid payer";
         if (remit.dst_wallet.size () != HASH_SIZE) throw "invalid receiver";
         if (remit.sign.size () != SIGN_SIZE) throw "invalid sign";
         message_list.push_back (remit);
      }
   }

   for (auto & msg : message_list) {
      buf hash { msg.hash () };
      if (m_db.check_message_hash (hash)) throw "doubled message";
   }

   for (auto & msg : message_list) {
      buf content { msg.serialize (false) };
      bool verify;
      if (msg.type == RMT_WITHDRAW) {
         verify = m_signer.verify (content, msg.sign, msg.dst_wallet);
      } else {
         verify = m_signer.verify (content, msg.sign, msg.src_wallet);
      }
      if (! verify) throw "wrong signature";
   }

   set <buf> wallets;
   qword fee_paid { 0 };
   for (auto & msg : message_list) {
      if (msg.type == RMT_FEE) fee_paid += msg.sum;
      if (msg.type != RMT_WITHDRAW) wallets.insert (msg.src_wallet);
      if (msg.type != RMT_DELEGATE && msg.type != RMT_FEE) wallets.insert (msg.dst_wallet);
   }

   qword fee_calculated { calculate_transaction_fee (message_list, wallets.size ()) };
   if (fee_calculated > fee_paid) {
      str s { "wrong request: the fee must be at least " };
      s += readable_amount (fee_calculated);
      throw s;
   }
   trx_rmt_t::ptr trx { new trx_rmt_t };
   for (auto & iter : message_list) {
      trx->message_list.push_back (iter);
   }

   m_rtrxs_recv.push (trx);

   timer::sleep (100 MILLISECONDS);
   result_.put ("trxid", trx->hash ().to_b58 ());
}

// =====================================================================================================================

namespace
{

void parse_fee (json_t & node_, vector <remit_t> & list_)
{
   remit_t msg;
   msg.type = RMT_FEE;
   for (auto & node : node_) {
      str nm { node.first };
      str vl { node.second.get_value <str> () };
      if (0) {
      } else if (nm == "time") {
         try { msg.time = boost::lexical_cast <qword> (vl); } catch (...) {}
      } else if (nm == "payer") {
         msg.src_wallet.from_b58 (vl);
      } else if (nm == "receiver") {
         msg.dst_wallet.from_b58 (vl);
      } else if (nm == "sum") {
         try { msg.sum = boost::lexical_cast <qword> (vl); } catch (...) {}
      } else if (nm == "comment") {
         if (vl.length () > 255) vl.resize (255);
         msg.comment = vl;
      } else if (nm == "sign") {
         msg.sign.from_b58 (vl);
      }
   }
   list_.push_back (msg);
}

// =====================================================================================================================

void parse_trx (trx_sys_t & trx_, json_t & result_)
{
   json_t children;
   result_.put ("trxid", trx_.hash ().to_b58 ());
   result_.put ("type", "system");
   result_.put ("status", trx_.state == TRX_STATE_APPROVED ? "approved" : "rejected");
   for (auto & ii : trx_.votes.cont ()) {
      json_t child;
      child.put ("block", ii.block);
      child.put ("round", ii.round);
      child.put ("author", ii.author.to_b58 ());
      child.put ("vote", ii.vote.to_b58 ());
      child.put ("sign", ii.sign.to_b58 ());
      children.push_back (std::make_pair ("", child));
   }
   if (! children.empty ()) { result_.add_child ("votes", children); children.clear (); }
   for (auto & ii : trx_.verifies.cont ()) {
      json_t child;
      child.put ("block", ii.block);
      child.put ("round", ii.round);
      child.put ("validator", ii.validator.to_b58 ());
      child.put ("author", ii.author.to_b58 ());
      child.put ("hash", ii.block_hash.to_b58 ());
      child.put ("result", ii.result ? "approved" : "rejected");
      if (! ii.result) child.put ("reason", ii.reason.size () == HASH_SIZE ? ii.reason.to_b58 () : ii.reason.as_str ());
      child.put ("sign", ii.sign.to_b58 ());
      children.push_back (std::make_pair ("", child));
   }
   if (! children.empty ()) { result_.add_child ("verification", children); children.clear (); }
   for (auto & ii : trx_.confirms.cont ()) {
      json_t child;
      str type;
      switch (ii.type) {
         case LEADER_SCREWED_UP_OFFLINE: type = "leader offline"; break;
         case LEADER_SCREWED_UP_RETARD:  type = "leader retards"; break;
         case LEADER_SCREWED_UP_MISLEAD: type = "node misleads"; break;
      }
      child.put ("type", type);
      child.put ("stage", ii.stage == STAGE_VOTING ? "voting" : "validating");
      child.put ("block", ii.block);
      child.put ("round", ii.round);
      child.put ("author", ii.author.to_b58 ());
      child.put ("node", ii.causer.to_b58 ());
      child.put ("sign", ii.sign.to_b58 ());
      children.push_back (std::make_pair ("", child));
   }
   if (! children.empty ()) { result_.add_child ("confirmations", children); children.clear (); }
   for (auto & msg : trx_.disqualifications.cont ()) {
      json_t child;
      child.put ("stage", msg.stage == STAGE_VOTING ? "voting" : "validating");
      child.put ("block", msg.block);
      child.put ("round", msg.round);
      child.put ("author", msg.author.to_b58 ());
      child.put ("disqualified", msg.node.to_b58 ());
      child.put ("until block", msg.until);
      child.put ("sign", msg.sign.to_b58 ());
      
      word count { msg.confirms.fetch <word> () };
      confirms_leader_screwed_t confirms;
      verify_results_t results;
      for (word ii = 0; ii < count; ++ ii) {
         msg_type_t type { static_cast <msg_type_t> (msg.confirms.peek <word> ()) };
         if (type == MSG_CNF_LEADER_SCREWED_UP) {
            confirms.append (confirm_leader_screwed_t { msg.confirms, true });
         } else if (type == MSG_REP_BLOCK_VERIFIED) {
            results.append (verify_result_t { msg.confirms, true });
         }
      }
      json_t children2;
      for (auto & ii : confirms.cont ()) {
         json_t child2;
         str type;
         switch (ii.type) {
            case LEADER_SCREWED_UP_OFFLINE: type = "leader offline"; break;
            case LEADER_SCREWED_UP_RETARD:  type = "leader retards"; break;
            case LEADER_SCREWED_UP_MISLEAD: type = "node misleads"; break;
         }
         child2.put ("type", type);
         child2.put ("stage", ii.stage == STAGE_VOTING ? "voting" : "validating");
         child2.put ("block", ii.block);
         child2.put ("round", ii.round);
         child2.put ("author", ii.author.to_b58 ());
         child2.put ("node", ii.causer.to_b58 ());
         child2.put ("sign", ii.sign.to_b58 ());
         children2.push_back (std::make_pair ("", child2));
      }
      if (! children2.empty ()) { child.add_child ("confirms", children2); children2.clear (); }
      for (auto & ii : results.cont ()) {
         json_t child2;
         child2.put ("block", ii.block);
         child2.put ("round", ii.round);
         child2.put ("validator", ii.validator.to_b58 ());
         child2.put ("author", ii.author.to_b58 ());
         child2.put ("hash", ii.block_hash.to_b58 ());
         child2.put ("result", ii.result ? "approved" : "rejected");
         if (! ii.result) child2.put ("reason", ii.reason.size () == HASH_SIZE ? ii.reason.to_b58 () : ii.reason.as_str ());
         child2.put ("sign", ii.sign.to_b58 ());
         children2.push_back (std::make_pair ("", child2));
      }
      if (! children2.empty ()) { child.add_child ("verifications", children2); children2.clear (); }
      children.push_back (std::make_pair ("", child));
   }
   if (! children.empty ()) { result_.add_child ("disqualifications", children); children.clear (); }
}

// =====================================================================================================================

void parse_trx (trx_rmt_t & trx_, json_t & result_)
{
   json_t children;
   result_.put ("trxid", trx_.hash ().to_b58 ());
   result_.put ("type", "remittance");
   result_.put ("status", trx_state_text [trx_.state]);
   for (auto & msg : trx_.message_list) {
      json_t child;
      child.put ("type", trx_msg_text [msg.type]);
      child.put ("time", msg.time);
      child.put ("payer", msg.src_wallet.to_b58 ());
      child.put ("receiver", msg.dst_wallet.to_b58 ());
      child.put ("sum", msg.sum);
      child.put ("comment", msg.comment.as_str ());
      child.put ("sign", msg.sign.to_b58 ());
      children.push_back (std::make_pair ("", child));
   }
   result_.add_child ("msgs", children);
}

// =====================================================================================================================

void parse_trx (trx_nft_t & trx_, json_t & result_)
{
   result_.put ("trxid", trx_.hash ().to_b58 ());
   result_.put ("type", "nft");
   result_.put ("status", trx_state_text [trx_.state]);
   str action { trx_msg_text [trx_.type] };
   switch (trx_.type) {
      case NFT_CREATE:
      {
         nft_create_t & data { * trx_.token_create.get () };
         nft_info_t & token { data.token };
         json_t child;
         child.put ("id", token.id.to_b58 ());
         child.put ("coll", token.coll.to_b58 ());
         child.put ("minter", token.owner.to_b58 ());
         child.put ("metadata", token.meta);
         result_.add_child ("token", child);
         break;
      }
      case NFT_AUCTION:
      {
         nft_auction_t & data { * trx_.token_auction.get () };
         nft_info_t & token { data.token };
         action.append (": ").append (auction_state_text [data.state]);
         switch (data.state) {
            case AUCTION_ANNOUNCED:
            {
               json_t child;
               child.put ("time", data.time);
               child.put ("token", token.id.to_b58 ());
               child.put ("beneficiary", data.beneficiary.to_b58 ());
               child.put ("starting", data.starting);
               child.put ("duration", data.duration);
               child.put ("timeout", data.timeout);
               child.put ("start bid", data.start_bid);
               child.put ("bid step", data.bid_step);
               child.put ("sign", data.sign.to_b58 ());
               result_.add_child ("auction", child);
               break;
            }
            case AUCTION_COMPLETED:
            case BIDDING_STARTED:
            case BIDDING_FINISHED:
            case AUCTION_ABORTED:
            case AUCTION_CANCELLED:
            {
               break;
            }
         }
         break;
      }
      case NFT_BID:
      {
         nft_bid_t & data { * trx_.token_bid.get () };
         nft_info_t & token { data.token };
         json_t child;
         child.put ("time", data.time);
         child.put ("token", token.id.to_b58 ());
         child.put ("bidder", data.bidder.to_b58 ());
         child.put ("bid", data.bid);
         child.put ("sign", data.sign.to_b58 ());
         result_.add_child ("bid", child);
         break;
      }
      case NFT_TRANSFER:
      {
         nft_transfer_t & data { * trx_.token_transfer.get () };
         nft_info_t & token { data.token };
         json_t child;
         child.put ("time", data.time);
         child.put ("token", token.id.to_b58 ());
         child.put ("recipient", data.recipient.to_b58 ());
         child.put ("sign", data.sign.to_b58 ());
         result_.add_child ("transfer", child);
         break;
      }
      case NFT_ABORT:
      {
         nft_abort_t & data { * trx_.token_abort.get () };
         nft_info_t & token { data.token };
         json_t child;
         child.put ("time", data.time);
         child.put ("token", token.id.to_b58 ());
         child.put ("sign", data.sign.to_b58 ());
         result_.add_child ("abort", child);
         break;
      }
      case NFT_DONATE:
      {
         nft_donate_t & data { * trx_.token_donate.get () };
         nft_info_t & token { data.token };
         json_t child;
         child.put ("time", data.time);
         child.put ("token", token.id.to_b58 ());
         child.put ("recipient", data.recipient.to_b58 ());
         child.put ("sign", data.sign.to_b58 ());
         result_.add_child ("donate", child);
         break;
      }
      case NFT_BURN:
      {
         nft_burn_t & data { * trx_.token_burn.get () };
         nft_info_t & token { data.token };
         json_t child;
         child.put ("time", data.time);
         child.put ("token", token.id.to_b58 ());
         child.put ("sign", data.sign.to_b58 ());
         result_.add_child ("burn", child);
         break;
      }
   }
   result_.put ("action", action);

   for (auto & msg : trx_.message_list) {
      json_t child;
      child.put ("type", "fee");
      child.put ("time", msg.time);
      child.put ("payer", msg.src_wallet.to_b58 ());
      child.put ("receiver", msg.dst_wallet.to_b58 ());
      child.put ("sum", msg.sum);
      child.put ("comment", msg.comment.as_str ());
      child.put ("sign", msg.sign.to_b58 ());
      result_.add_child ("fee", child);
   }
}

// =====================================================================================================================

void parse_trx (trx_fft_t & trx_, json_t & result_)
{
   result_.put ("trxid", trx_.hash ().to_b58 ());
   result_.put ("type", "fft");
   result_.put ("status", trx_state_text [trx_.state]);
   str action { trx_msg_text [trx_.type] };
   switch (trx_.type) {
      case FFT_EMISSION:
      {
         fft_emission_t & data { * trx_.token_emission.get () };
         fft_info_t & token { data.token };
         json_t child;
         child.put ("id", token.id.to_b58 ());
         child.put ("owner", token.owner.to_b58 ());
         child.put ("name", token.name);
         child.put ("amount", data.amount);
         child.put ("wallet", data.wallet.to_b58 ());
         result_.add_child ("emission", child);
         break;
      }
      case NFT_TRANSFER:
      {
         fft_transfer_t & data { * trx_.token_transfer.get () };
         fft_info_t & token { data.token };
         json_t child;
         child.put ("time", data.time);
         child.put ("token", token.id.to_b58 ());
         child.put ("payer", data.payer.to_b58 ());
         child.put ("recipient", data.recipient.to_b58 ());
         child.put ("amount", data.amount);
         child.put ("sign", data.sign.to_b58 ());
         result_.add_child ("transfer", child);
         break;
      }
      case FFT_BURN:
      {
         fft_burn_t & data { * trx_.token_burn.get () };
         fft_info_t & token { data.token };
         json_t child;
         child.put ("time", data.time);
         child.put ("token", token.id.to_b58 ());
         child.put ("sign", data.sign.to_b58 ());
         result_.add_child ("burn", child);
         break;
      }
   }
   result_.put ("action", action);

   for (auto & msg : trx_.message_list) {
      json_t child;
      child.put ("type", "fee");
      child.put ("time", msg.time);
      child.put ("payer", msg.src_wallet.to_b58 ());
      child.put ("receiver", msg.dst_wallet.to_b58 ());
      child.put ("sum", msg.sum);
      child.put ("comment", msg.comment.as_str ());
      child.put ("sign", msg.sign.to_b58 ());
      result_.add_child ("fee", child);
   }
}

}

// =====================================================================================================================

void worker_t::json_rpc_get_trx_info (json_t & params_, json_t & result_)
{
   if (params_.empty ()) throw "missed param";

   buf trxid;
   for (auto & node : params_) {
      str nm { node.first };
      str vl { node.second.get_value <str> () };
      if (0) {
      } else if (nm == "trxid") {
         trxid.from_b58 (vl);
      }
   }

   if (trxid.size () != HASH_SIZE) throw "invalid trxid";

   trx_rmt_t::ptr rtrx { m_rtrxs_new.locate (trxid) };
   trx_sys_t::ptr strx;
   trx_nft_t::ptr ntrx;
   trx_fft_t::ptr ftrx;

   if (! rtrx) {
      trx_loc_t loc { m_db.locate_trx (trxid) };
      if (loc.pos) {
         buf block;
         if (is_block_keeper (m_me, loc.block)) {
            block = m_db.retrieve_block (loc.block);
         } else {
            block = get_block (loc.block)->block;
         }
         if (! block.empty ()) {
            block_header_t hdr { block, false };
            block.fetch_from_tail (SIGN_SIZE);
            for (dword ii = 0; ii < loc.pos; ++ ii) {
               block.fetch (nullptr, block.fetch <dword> ());
            }
            buf b { block.fetch (block.fetch <dword> ()) };
            trx_type_t type { static_cast <trx_type_t> (b.peek <word> ()) };
            if (type == TRX_TYPE_REMITTANCE)  rtrx.reset (new trx_rmt_t { b });
            else if (type == TRX_TYPE_SYSTEM) strx.reset (new trx_sys_t { b });
            else if (type == TRX_TYPE_NFT)    ntrx.reset (new trx_nft_t { m_db, b });
            else if (type == TRX_TYPE_FFT)    ftrx.reset (new trx_fft_t { m_db, b });
         }
      }
   }
   if (rtrx) parse_trx (* rtrx.get (), result_);
   else if (strx) parse_trx (* strx.get (), result_);
   else if (ntrx) parse_trx (* ntrx.get (), result_);
   else if (ftrx) parse_trx (* ftrx.get (), result_);
   else throw "trxid not [YET] found";
}

// ==================================================================================================================

void worker_t::json_rpc_get_nodelist (json_t & json_, json_t & result_)
{
   for (auto & node : m_nodelist.cont ()) {
      json_t j;
      j.put ("nodeid", node->nodeid_b58);
      j.put ("public", node->pubkey.to_b58 ());
      j.put ("stake", node->stake);
      j.put ("ip address", node->ipaddr);
      j.put ("ip port", node->ipport);
      str s { std::to_string (node->storage_seed) };
      s.append ("-").append (std::to_string (node->storage_arity));
      j.put ("storage", s);
      j.put ("comment", node->comment);
      result_.push_back (std::make_pair ("", j));
   }
}

// ==================================================================================================================

void worker_t::json_rpc_get_leader (json_t & json_, json_t & result_)
{
   node_t * leader { m_stage == STAGE_VOTING || m_stage == STAGE_WAITING ? who_voting_lead () : who_valing_lead () };
   result_.put ("nodeid", leader ? leader->nodeid_b58 : "yet undefined");
}

// ==================================================================================================================

void worker_t::json_rpc_get_wallet_trxs (json_t & params_, json_t & result_)
{
   if (params_.empty ()) throw "missed param";

   buf walid;
   bool all = false;
   for (auto & node : params_) {
      str nm { node.first };
      str vl { node.second.get_value <str> () };
      if (0) {
      } else if (nm == "wallet") {
         walid.from_b58 (vl);
      } else if (nm == "all") {
         all = true;
      }
   }

   wallet_t wallet { m_db.get_wallet (walid) };
   if (wallet.last_trx.empty ()) throw "wallet not found";
   qword last_block = all ? m_next_block_to_process - 1 : [&] ()
                                                          {
                                                             trx_loc_t last_trx { m_db.locate_trx (wallet.last_trx) };
                                                             vector <trx_rmt_t> trxs;
                                                             return last_trx.block;
                                                          } ();
   for (qword block_no { 0 }; block_no <= last_block; ++ block_no) {
      block_t::ptr blk { get_block (block_no) };
      if (! blk || blk->block.empty ()) throw "block not found";
      buf block { blk->block };
      block_header_t hdr { block, false };
      block.fetch_from_tail (SIGN_SIZE);
      while (! block.empty ()) {
         buf btrx { block.fetch (block.fetch <dword> ()) };
         json_t jtrx;
         switch (static_cast <trx_type_t> (btrx.peek <word> ())) {
            case TRX_TYPE_REMITTANCE:
            {
               bool skip { true };
               trx_rmt_t trx { btrx };
               for (auto & wal : trx.wallet_list) {
                  if (wal.id == walid) { skip = false; break; }
               }
               if (skip) continue;
               if (trx.state == TRX_STATE_REJECTED && ! all) continue;
               parse_trx (trx, jtrx);
               break;
            }
            case TRX_TYPE_NFT:
            {
               bool skip { true };
               trx_nft_t trx { m_db, btrx };
               for (auto & wal : trx.wallet_list) {
                  if (wal.id == walid) { skip = false; break; }
               }
               if (skip) continue;
               if (trx.state == TRX_STATE_REJECTED && ! all) continue;
               parse_trx (trx, jtrx);
               break;
            }
            case TRX_TYPE_FFT:
            {
               bool present { false };
               trx_fft_t trx { m_db, btrx };
               for (auto & wal : trx.wallet_list) {
                  if (wal.id == walid) { present = true; break; }
               }
               if (! present) for (auto & wal : trx.fft_wallet_list) {
                  if (wal.id == walid) { present = true; break; }
               }
               if (! present) continue;
               if (trx.state == TRX_STATE_REJECTED && ! all) continue;
               parse_trx (trx, jtrx);
               break;
            }
            default:
            {
               continue;
            }
         }
         result_.push_back (std::make_pair ("", jtrx));
      }
   }
}

// ==================================================================================================================

void worker_t::json_rpc_get_wallet_state (json_t & params_, json_t & result_)
{
   if (params_.empty ()) throw "missed param";

   buf walid;
   for (auto & node : params_) {
      str nm { node.first };
      str vl { node.second.get_value <str> () };
      if (0) {
      } else if (nm == "wallet") {
         walid.from_b58 (vl);
      }
   }

   wallet_t wallet { m_db.get_wallet (walid) };
   if (! wallet.last_trx.empty ()) {
      result_.put ("wallet", wallet.id.to_b58 ());
      result_.put ("balance", wallet.balance);
      if (wallet.blocked) result_.put ("blocked", wallet.blocked);
      result_.put ("last trx", wallet.last_trx.to_b58 ());
   }

   json_t children;
   for (auto & wal : m_db.get_fft_wallet (walid)) {
      json_t child;
      child.put ("token", wal.token.to_b58 ());
      child.put ("balance", wal.balance);
      children.push_back (std::make_pair ("", child));
   }
   if (! children.empty ()) result_.add_child ("tokens", children);
}

// =====================================================================================================================

void worker_t::json_rpc_get_block (json_t & params_, json_t & result_)
{
   qword block_no { m_next_block_to_process - 1 };
   for (auto & node : params_) {
      str nm { node.first };
      str vl { node.second.get_value <str> () };
      if (0) {
      } else if (nm == "blk") {
         try { block_no = boost::lexical_cast <qword> (vl); } catch (...) {}
      }
   }
   block_t::ptr blk { get_block (block_no) };
   if (! blk) throw "not found";

   buf block { blk->block };
   buf hash { sha256 (block) };
   block_header_t hdr { block, true };
   buf sign { block.fetch_from_tail (SIGN_SIZE) };
   node_t * validator { m_nodelist.find (hdr.validator) };;
   if (! validator) throw "unknown validator";
   if (! m_signer.verify (block, sign, validator->pubkey)) throw "bad signature";
   block.fetch (block_header_t::size);
   json_t children;
   result_.put ("blk", hdr.block);
   result_.put ("blkid", hash.to_b58 ());
   result_.put ("validator", hdr.validator.to_b58 ());

   while (! block.empty ()) {
      buf btrx { block.fetch (block.fetch <dword> ()) };
      trx_type_t type = static_cast <trx_type_t> (btrx.peek <word> ());
      switch (type) {
         case TRX_TYPE_SYSTEM:
         {
            trx_sys_t trx { btrx };
            json_t jtrx;
            parse_trx (trx, jtrx);
            children.push_back (std::make_pair ("", jtrx));
            break;
         }
         case TRX_TYPE_REMITTANCE:
         {
            trx_rmt_t trx { btrx };
            json_t jtrx;
            parse_trx (trx, jtrx);
            children.push_back (std::make_pair ("", jtrx));
            break;
         }
         case TRX_TYPE_NFT:
         {
            trx_nft_t trx { m_db, btrx };
            json_t jtrx;
            parse_trx (trx, jtrx);
            children.push_back (std::make_pair ("", jtrx));
            break;
         }
         case TRX_TYPE_FFT:
         {
            trx_fft_t trx { m_db, btrx };
            json_t jtrx;
            parse_trx (trx, jtrx);
            children.push_back (std::make_pair ("", jtrx));
            break;
         }
      }
   }
   result_.add_child ("trxs", children);
}

// =====================================================================================================================

void worker_t::json_rpc_nft_create (json_t & params_, json_t & result_)
{
   if (params_.empty ()) throw "missed param";
   trx_nft_t::ptr ptrx { new trx_nft_t { NFT_CREATE } };
   trx_nft_t & trx { * ptrx.get () };
   for (auto & node : params_) {
      if (node.first == "data") {
         trx.token_create.reset (new nft_create_t);
         nft_create_t & msg { * trx.token_create.get () };
         for (auto & node : node.second) {
            str nm { node.first };
            str vl { node.second.get_value <str> () };
            if (0) {
            } else if (nm == "time") {
               try { msg.time = boost::lexical_cast <qword> (vl); } catch (...) {}
            } else if (nm == "coll") {
               msg.token.coll.from_b58 (vl);
            } else if (nm == "token") {
               msg.token.id.from_b58 (vl);
            } else if (nm == "minter") {
               msg.token.owner.from_b58 (vl);
            } else if (nm == "metadata") {
               msg.token.meta = vl;
            } else if (nm == "sign") {
               msg.sign.from_b58 (vl);
            }
         }
      } else if (node.first == "fee") {
         parse_fee (node.second, trx.message_list);
      }
   }

   val_data_t data_blk, data_trx;
   validate_trx_nft_create (data_blk, data_trx, trx);
   m_ntrxs_recv.push (ptrx);
   result_.put ("trxid", trx.hash ().to_b58 ());
}


// =====================================================================================================================

void worker_t::json_rpc_nft_donate (json_t & params_, json_t & result_)
{
   if (params_.empty ()) throw "missed param";
   trx_nft_t::ptr ptrx { new trx_nft_t { NFT_DONATE } };
   trx_nft_t & trx { * ptrx.get () };
   for (auto & node : params_) {
      if (node.first == "data") {
         trx.token_donate.reset (new nft_donate_t);
         nft_donate_t & msg { * trx.token_donate.get () };
         for (auto & node : node.second) {
            str nm { node.first };
            str vl { node.second.get_value <str> () };
            if (0) {
            } else if (nm == "time") {
               try { msg.time = boost::lexical_cast <qword> (vl); } catch (...) {}
            } else if (nm == "token") {
               msg.token.id.from_b58 (vl);
            } else if (nm == "recipient") {
               msg.recipient.from_b58 (vl);
            } else if (nm == "sign") {
               msg.sign.from_b58 (vl);
            }
         }
      } else if (node.first == "fee") {
         parse_fee (node.second, trx.message_list);
      }
   }

   val_data_t data_blk, data_trx;
   validate_trx_nft_donate (data_blk, data_trx, trx);
   m_ntrxs_recv.push (ptrx);
   result_.put ("trxid", trx.hash ().to_b58 ());
}

// =====================================================================================================================

void worker_t::json_rpc_nft_burn (json_t & params_, json_t & result_)
{
   if (params_.empty ()) throw "missed param";
   trx_nft_t::ptr ptrx { new trx_nft_t { NFT_BURN } };
   trx_nft_t & trx { * ptrx.get () };
   for (auto & node : params_) {
      if (node.first == "data") {
         trx.token_burn.reset (new nft_burn_t);
         nft_burn_t & msg = * trx.token_burn.get ();
         for (auto & node : node.second) {
            str nm { node.first };
            str vl { node.second.get_value <str> () };
            if (0) {
            } else if (nm == "time") {
               try { msg.time = boost::lexical_cast <qword> (vl); } catch (...) {}
            } else if (nm == "token") {
               msg.token.id.from_b58 (vl);
            } else if (nm == "sign") {
               msg.sign.from_b58 (vl);
            }
         }
      } else if (node.first == "fee") {
         parse_fee (node.second, trx.message_list);
      }
   }

   val_data_t data_blk, data_trx;
   validate_trx_nft_burn (data_blk, data_trx, trx);
   m_ntrxs_recv.push (ptrx);
   result_.put ("trxid", trx.hash ().to_b58 ());
}

// =====================================================================================================================

void worker_t::json_rpc_get_coll_info (json_t & params_, json_t & result_)
{
   if (params_.empty ()) throw "missed param";

   buf coll;
   for (auto & node : params_) {
      str nm { node.first };
      str vl { node.second.get_value <str> () };
      if (0) {
      } else if (nm == "coll") {
         coll.from_b58 (vl);
      }
   }
   if (coll.size () != HASH_SIZE) throw "wrong coll";
   vector <buf> tokens { m_db.get_nfts (coll) };
   for (auto & tok : tokens) {
      json_t child;
      child.put ("", tok.to_b58 ());
      result_.push_back (std::make_pair ("", child));
   }
}

// =====================================================================================================================

void worker_t::json_rpc_get_nft_info (json_t & params_, json_t & result_)
{
   if (params_.empty ()) throw "missed param";

   buf token;
   for (auto & node : params_) {
      str nm { node.first };
      str vl { node.second.get_value <str> () };
      if (0) {
      } else if (nm == "token") {
         token.from_b58 (vl);
      }
   }
   if (token.size () != HASH_SIZE) throw "wrong token";
   nft_info_t tok { m_db.get_nft (token) };
   if (tok.last_trx.empty ()) throw "unknown token";
   result_.put ("token", tok.id.to_b58 ());
   result_.put ("coll", tok.coll.to_b58 ());
   result_.put ("owner", tok.owner.to_b58 ());
   result_.put ("metadata", tok.meta);
   result_.put ("last trx", tok.last_trx.to_b58 ());
}

// =====================================================================================================================

void worker_t::json_rpc_fft_emission (json_t & params_, json_t & result_)
{
   if (params_.empty ()) throw "missed param";
   trx_fft_t::ptr ptrx { new trx_fft_t { FFT_EMISSION } };
   trx_fft_t & trx { * ptrx.get () };
   for (auto & node : params_) {
      if (node.first == "data") {
         trx.token_emission.reset (new fft_emission_t);
         fft_emission_t & msg { * trx.token_emission.get () };
         for (auto & node : node.second) {
            str nm { node.first };
            str vl { node.second.get_value <str> () };
            if (0) {
            } else if (nm == "time") {
               try { msg.time = boost::lexical_cast <qword> (vl); } catch (...) {}
            } else if (nm == "token") {
               msg.token.id.from_b58 (vl);
            } else if (nm == "token name") {
               msg.token.name = vl;
            } else if (nm == "wallet") {
               msg.wallet.from_b58 (vl);
            } else if (nm == "owner") {
               msg.token.owner.from_b58 (vl);
            } else if (nm == "amount") {
               try { msg.amount = boost::lexical_cast <qword> (vl); } catch (...) {}
            } else if (nm == "sign") {
               msg.sign.from_b58 (vl);
            }
         }
      } else if (node.first == "fee") {
         parse_fee (node.second, trx.message_list);
      }
   }

   val_data_t data_blk, data_trx;
   validate_trx_fft_emission (data_blk, data_trx, trx);
   m_ftrxs_recv.push (ptrx);
   result_.put ("trxid", trx.hash ().to_b58 ());
}

// =====================================================================================================================

void worker_t::json_rpc_fft_transfer (json_t & params_, json_t & result_)
{
   if (params_.empty ()) throw "missed param";
   trx_fft_t::ptr ptrx { new trx_fft_t { FFT_TRANSFER } };
   trx_fft_t & trx { * ptrx.get () };
   for (auto & node : params_) {
      if (node.first == "data") {
         trx.token_transfer.reset (new fft_transfer_t);
         fft_transfer_t & msg { * trx.token_transfer.get () };
         for (auto & node : node.second) {
            str nm { node.first };
            str vl { node.second.get_value <str> () };
            if (0) {
            } else if (nm == "time") {
               try { msg.time = boost::lexical_cast <qword> (vl); } catch (...) {}
            } else if (nm == "token") {
               msg.token.id.from_b58 (vl);
            } else if (nm == "payer") {
               msg.payer.from_b58 (vl);
            } else if (nm == "recipient") {
               msg.recipient.from_b58 (vl);
            } else if (nm == "amount") {
               try { msg.amount = boost::lexical_cast <qword> (vl); } catch (...) {}
            } else if (nm == "sign") {
               msg.sign.from_b58 (vl);
            }
         }
      } else if (node.first == "fee") {
         parse_fee (node.second, trx.message_list);
      }
   }

   val_data_t data_blk, data_trx;
   validate_trx_fft_transfer (data_blk, data_trx, trx);
   m_ftrxs_recv.push (ptrx);
   result_.put ("trxid", trx.hash ().to_b58 ());
}

// =====================================================================================================================

void worker_t::json_rpc_fft_burn (json_t & params_, json_t & result_)
{
   if (params_.empty ()) throw "missed param";
   trx_fft_t::ptr ptrx { new trx_fft_t { FFT_BURN } };
   trx_fft_t & trx { * ptrx.get () };
   for (auto & node : params_) {
      if (node.first == "data") {
         trx.token_burn.reset (new fft_burn_t);
         fft_burn_t & msg { * trx.token_burn.get () };
         for (auto & node : node.second) {
            str nm { node.first };
            str vl { node.second.get_value <str> () };
            if (0) {
            } else if (nm == "time") {
               try { msg.time = boost::lexical_cast <qword> (vl); } catch (...) {}
            } else if (nm == "token") {
               msg.token.id.from_b58 (vl);
            } else if (nm == "wallet") {
               msg.wallet.from_b58 (vl);
            } else if (nm == "amount") {
               try { msg.amount = boost::lexical_cast <qword> (vl); } catch (...) {}
            } else if (nm == "sign") {
               msg.sign.from_b58 (vl);
            }
         }
      } else if (node.first == "fee") {
         parse_fee (node.second, trx.message_list);
      }
   }

   val_data_t data_blk, data_trx;
   validate_trx_fft_burn (data_blk, data_trx, trx);
   m_ftrxs_recv.push (ptrx);
   result_.put ("trxid", trx.hash ().to_b58 ());
}

// =====================================================================================================================

void worker_t::json_rpc_get_fft_list (json_t & params_, json_t & result_)
{
   vector <buf> list { m_db.get_ffts () };
   for (auto & item : list) {
      json_t child;
      child.put ("", item.to_b58 ());
      result_.push_back (std::make_pair ("", child));
   }
}

// =====================================================================================================================

void worker_t::json_rpc_get_fft_info (json_t & params_, json_t & result_)
{
   if (params_.empty ()) throw "missed param";

   buf token;
   for (auto & node : params_) {
      str nm { node.first };
      str vl { node.second.get_value <str> () };
      if (0) {
      } else if (nm == "token") {
         token.from_b58 (vl);
      }
   }
   if (token.size () != HASH_SIZE) throw "wrong token";
   fft_info_t tok { m_db.get_fft (token) };
   if (tok.owner.empty ()) throw "unknown token";
   result_.put ("token", tok.id.to_b58 ());
   result_.put ("owner", tok.owner.to_b58 ());
   result_.put ("name", tok.name);
}

// =====================================================================================================================
