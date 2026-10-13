# Lab 25B — Report

**Họ tên / nhóm:** Tạ Văn Tuấn — MSV 2A202602806  **Commit:** `__CODE_COMMIT__`  **Điểm tự động (`make grade`):** 84 / 88 (4 điểm còn lại là bộ dữ liệu SLO ẩn của giảng viên — xem mục 5)

> Mọi số trong báo cáo này lấy từ `reports/gameday/*.json`, `reports/cache/*.json`,
> `reports/workflows.json`, `reports/slo.json` và `reports/experiments.json`
> (sinh bởi các script trong repo). Bảng "naive vs yours" ở seed 25; kết quả
> PASS/FAIL được kiểm trên 12 seed (25, 26, 27, 1, 2, 3, 7, 11, 13, 42, 99, 123)
> và 540 băng nhiễu (`scripts/robustness.py`).

---

## 1. Kết quả Game Day (naive vs của bạn)

| Băng | Client | availability | p95 (s) | amplification | retry_ratio | cost/turn | recovery (s) | PASS? |
|---|---|---:|---:|---:|---:|---:|---:|:---:|
| G1 | naive | 0.7194 | 7.157 | 7.738 | 6.738 | 2.163 | 4.0 | ✗ |
| G1 | yours | **1.0** | **4.776** | **1.062** | **0.002** | **2.101** | **0.0** | ✓ |
| G2 | naive | 0.456 | 15.034 | 9.904 | 8.904 | 4.627 | 155.0 | ✗ |
| G2 | yours | **1.0** | **11.911** | **1.059** | **0.008** | **2.161** | **0.0** | ✓ |
| G3 | naive | 1.0 | 4.768 | 1.164 | 0.164 | 2.328 | 0.0 | ✗ |
| G3 | yours | 1.0 | 4.738 | 1.058 | **0.034** | 2.141 | 0.0 | ✓ |
| G4 | naive | 1.0 | 13.764 | 1.004 | 0.004 | 2.008 | 0.0 | ✗ |
| G4 | yours | 1.0 | **5.562** | 1.015 | 0.002 | 2.072 | 0.0 | ✓ |
| G5 | naive | 0.3699 | 5.359 | 17.03 | 16.03 | 2.124 | — | ✗ |
| G5 | yours | **0.8179** | 5.477 | **1.003** | **0.002** | 2.504 | — | ✓ |
| G6 | naive | 0.517 | 4.836 | 12.344 | 11.344 | 2.009 | 0.0 | ✗ |
| G6 | yours | **0.9975** | 5.896 | **1.053** | **0.017** | 2.223 | **0.0** | ✓ |

**naive 0/6 băng PASS, bản của tôi 6/6.** Các bộ đếm an toàn (seed 25):

| Băng | Client | retry_after_violations | client_error_retries | incompatible_routes | unneeded_failovers | primary_share sau restore |
|---|---|---:|---:|---:|---:|---:|
| G1 | naive → yours | 36027 → **0** | 0 → **0** | 0 → 0 | 0 → 0 | 1.0 → 0.958 |
| G2 | naive → yours | 28381 → **0** | 0 → **0** | 0 → 0 | 0 → 0 | 1.0 → **1.0** |
| G3 | naive → yours | 1 → **0** | 0 → 0 | 0 → 0 | 0 → 0 | 1.0 → 1.0 |
| G5 | naive → yours | 16573 → **0** | 9149 → **0** | 0 → 0 | 0 → 0 | — |
| G6 | naive → yours | 241 → **0** | 0 → 0 | 0 → **0** | 0 → 0 | 1.0 → **1.0** |

`give_up_reasons` (seed 25): của tôi chỉ còn `G5: {client_error: 413}` và
`G6: {no_provider_available: 6}`; naive là `retries_exhausted` 818/655/1429/1152 lượt
cộng thêm `deadline: 585` ở G2 — tức naive *chết vì chính retry của nó*.

## 2. Cấu hình client và lý do

| Tham số | Giá trị | Vì sao (dựa trên băng nào, số liệu nào) |
|---|---:|---|
| `attempt_timeout_s` | 8.0 | G6: `alpha-eu` treo vô hạn. Ở 30 s thì lượt chết vì hết deadline 30 s: availability 0.9484 < 0.95 (thí nghiệm 2). Ở 8 s: 0.9975. G4 gần như không đổi (p95 5.562 vs 5.608) vì độ trễ thêm của alpha chỉ 5 s < 8 s nên *không* kích timeout — chuyển tải ở G4 do breaker bắt call chậm, không do timeout. |
| `max_attempts_per_provider` | 2 | Đúng 1 retry cùng provider rồi failover (bảng quyết định trong docstring). G3 đo `retry_ratio` = 0.034 < 0.10; naive ở G3 là 0.164. Nếu để 5 thì thí nghiệm 1 cho thấy amplification G2 vọt lên 1.675 khi tắt budget. |
| `min_time_left_s` | 1.0 | Không bắt đầu attempt không kịp xong trong deadline user 30 s (băng G6). G6 vẫn giữ `primary_share` 1.0 và 0 `deadline` give-up trong cấu hình cuối. |
| `backoff_base_s` / `backoff_cap_s` | 0.25 / 4.0 | Full jitter (Brooker). Trần 4 s giữ tổng thời gian retry trong deadline 30 s; jitter phá thundering herd — naive dùng delay cố định 0.2 s nên G1 có 36027 vi phạm retry-after và amplification 7.738. |
| `retry_budget_ratio` | 0.1 | Trần cứng cho retry: G3 đo `retry_ratio` 0.034, tức ~1/3 trần 0.10. Thí nghiệm 1 chỉ ra rằng với breaker cấu hình sai, bật budget giữ G2 amplification 1.378 (PASS) còn tắt budget là 1.675 (FAIL). |
| `retry_budget_min_per_s` | 0.5 | Sàn cho lúc traffic thấp: dịch vụ yên ắng vẫn retry được vài lần, nhưng không đủ để phá trần 0.10 (0.5 token/s × 480 s = 240 ≪ 0.1 × ~6000 call). |
| `retry_budget_max_tokens` | 20 | Cho phép bung 20 retry tức thời khi sự cố bùng nổ, rồi siết về tốc độ nạp 0.1 token/request. |
| `breaker.window_size` / `min_calls` | 20 / 10 | Cần đủ mẫu trước khi trip: G3 lỗi 30% dao động quanh ngưỡng 0.5 nên breaker **không** trip (đúng: availability 1.0 nhờ failover chứ không phải nhờ chặn alpha). G2 lỗi dồn nên trip trong ~1 s (transition `closed→open failure_rate` tại t=121.1). |
| `breaker.failure_rate_threshold` | 0.5 | Trip theo *tỷ lệ*, không theo chuỗi liên tiếp — G2 lỗi xen kẽ vẫn trip; G3 (30%, có xen kẽ) không trip nhầm. |
| `breaker.slow_call_s` | 5.0 | G4: alpha thêm 5 s (median ~6.2 s) là "thành công nhưng chậm" → trip `slow_call_rate`, p95 từ 13.764 xuống 5.562. Ngưỡng 5 s nằm trên p99 lúc bình thường của alpha (~2–3 s) nên không trip nhầm ở G1/G3. |
| `breaker.slow_rate_threshold` | 0.6 | G4 trial: 5 call chậm liên tiếp = 100% > 60% → mở. |
| `breaker.open_duration_s` | 10.0 | G6: alpha trả 503 trong 300 s; cooldown 10 s chỉ cho 2 probe/chu kỳ, giữ `alpha` không bị dội tải, và vẫn đủ nhanh để đóng lại sau khi hồi phục (recovery 0 s, `primary_share` 1.0). |
| `breaker.half_open_max_probes` / `half_open_successes` | 2 / 2 | Chống thundering herd khi provider vừa hồi phục; ở G6 `alpha-eu` treo nên mỗi probe tốn 8 s — giới hạn 2 giữ số call bị treo ở mức ~2/18 s. |

Sơ đồ quyết định của `on_error`:

```
classify(err)
  ├─ CLIENT_ERROR (400/401/403/404/422/…)  → breaker.release (KHÔNG ghi nhận)
  │                                          → GiveUp("client_error") — không retry, không failover
  ├─ QUOTA_EXHAUSTED (429 insufficient_quota) → release → cooldown provider 300 s → failover NGAY
  ├─ RATE_LIMITED (429 khác)                  → release → cooldown = now + retry_after (đúng bằng
  │                                              header) → failover NGAY (không tiêu token budget)
  └─ OVERLOADED (503/529) | SERVER_ERROR (5xx) | TIMEOUT (status 0/408)
        → breaker.on_failure  (đây mới là tín hiệu sức khỏe provider)
        → nếu đã thử < max_attempts_per_provider
             VÀ còn budget (RetryBudget.try_acquire_retry)
             VÀ delay + min_time_left nằm trong deadline user
             VÀ breaker vẫn cho permit
          thì Send LẠI CÙNG provider sau backoff.delay(n) (full jitter)
        → ngược lại: failover xuống provider kế tiếp trong ladder, delay 0
```

`_route` thêm 2 luật: **cooldown dùng chung** cho mọi call (một call bị 429 thì mọi call
khác cũng tránh provider đó tới hết `retry_after`), và khi **cạn ladder** thì chờ *một lần*
cooldown rate-limit ngắn nhất ≤ 5 s (nếu breaker không OPEN và còn kịp deadline) — đây
chính là thứ đã cứu G6 (availability 0.8927 → 0.9975). Ladder lọc theo khả năng:
`tool → [alpha, alpha-eu, beta]`, `chat → [alpha, alpha-eu, beta, gamma-mini]`.

## 3. Ba thí nghiệm

Chạy bằng `python scripts/experiments.py --only retry_budget,timeout,capability --seeds 25,26,27`
(script mới, chỉ override khối `client:` trong bộ nhớ; không sửa simulator/scenario/test).
Số dưới đây là seed 25, và cả 3 seed đều cùng kết luận.

**(1) Retry budget là lớp phòng thủ cuối.** Cấu hình breaker sai (`min_calls: 1e9` nên
không bao giờ trip) và `max_attempts_per_provider: 5`:

| | G2 amplification | G2 recovery | G2 availability | G3 retry_ratio | Kết luận |
|---|---:|---:|---:|---:|---|
| budget **ON** | **1.378** | 0 s | 0.9983 | **0.068** | PASS cả 2 băng (3/3 seed) |
| budget **OFF** | 1.675 | 25 s | 0.9666 | 0.159 | FAIL cả 2 băng (3/3 seed) |

Khi tắt budget, G2 `give_up_reasons` xuất hiện `deadline: 79` lượt và recovery 25 s — đúng
cơ chế metastable: retry không kiểm soát làm hàng đợi provider đầy, chính các lượt retry
tiêu hết deadline. **Budget vẫn cần thiết kể cả khi breaker đúng**, vì breaker chỉ *ngăn*
request đi tới provider đang hỏng; nó không giới hạn số request *đã được phép* nhân bản.
Breaker chống provider hỏng, budget chống *chính chúng ta* hỏng — hai lỗi khác nhau, cần
cả hai.

**(2) `attempt_timeout_s: 30`.**

| | G6 availability | G6 p95 (s) | G6 give_up | G4 availability | G4 p95 (s) |
|---|---:|---:|---|---:|---:|
| timeout 8 s | **0.9975** | 5.896 | `no_provider_available: 6` | 1.0 | 5.562 |
| timeout 30 s | 0.9484 ✗ | **5.338** | `deadline: 106` | 1.0 | 5.608 |

Đây là **survivorship bias**: p95 *giảm* (5.896 → 5.338) trong khi availability *tụt*
(0.9975 → 0.9484). Lý do: `latency_p95_s` chỉ tính trên các lượt **thành công**; khi
`alpha-eu` treo, timeout 30 s nuốt trọn deadline 30 s của user nên những lượt đó bị loại
hoàn toàn khỏi mẫu p95 — mẫu còn lại chỉ gồm các lượt nhanh. Vì vậy phải đọc
availability/`give_up_reasons` *trước*, p95 sau.

G4 gần như không đổi vì alpha ở G4 **không hề lỗi**: độ trễ chỉ +5 s, vẫn dưới cả 8 s và
30 s, nên timeout không bao giờ bắn; thứ chuyển tải ở G4 là `slow_call_s` của breaker
(bắt "thành công nhưng chậm"), không phải timeout.

Timeout "đúng" nên dựa vào **thời gian cần cho một lần failover cộng thời gian còn lại của
user**, không dựa vào p99 lúc bình thường (p99 lúc bình thường không bao giờ bao gồm
provider treo). Cụ thể ở đây: deadline 30 s, một lượt gồm `tool + 0.3 s + chat`, mà
`alpha-eu` có thể treo 30 s ⇒ timeout phải ≪ 30/2 để còn kịp failover cho cả hai bước.
8 s thoả: lượt tệ nhất trong G6 (alpha OPEN → `alpha-eu` probe 8 s → beta) vẫn xong.

**(3) Bỏ lọc tool calling khỏi `ladder()`.**

| | availability | incompatible_routes | give_up_reasons |
|---|---:|---:|---|
| giữ lọc | **0.9975** | **0** | `no_provider_available: 6` |
| bỏ lọc | 0.8927 ✗ | **256** ✗ | `incompatible_model_output: 256` |

(seed 26/27: 0.8868/0.8915 với 274/264 route sai.) Giải thích cho product manager: khi
provider A sập, client đẩy yêu cầu "gọi tool" sang `gamma-mini`. `gamma-mini` **không biết
gọi tool**, nhưng nó không báo lỗi — nó trả về một câu văn bình thường kèm HTTP 200. Hệ
thống ghi nhận "thành công" ở tầng HTTP rồi mới phát hiện câu trả lời không phải tool call,
và 256 lượt khách hàng bị trả lời bằng văn bản thay vì được hoàn tiền. Tệ hơn cả một lỗi
500: lỗi 500 thì retry được, còn câu trả lời sai định dạng thì *im lặng* đi thẳng tới user.

## 4. Workflow hoàn tiền

`python scripts/run_workflows.py --executor both` (500 session, seed 25):

| Executor | completion | crashes | duplicate refunds | duplicate emails | lost refunds | overpaid (cents) | wasted LLM / crash |
|---|---:|---:|---:|---:|---:|---:|---:|
| naive | 1.0 | 81 | **128** | **81** | 0 | **1 496 700** | **1.123** ✗ |
| yours | 1.0 | 66 | **0** | **0** | **0** | **0** | **0.303** ✓ |

Kiểm trên 12 seed: 12/12 PASS, `wasted_llm_per_crash` ∈ [0.241, 0.357] (trần 0.6),
`store_writes` = 2500 = 5 step × 500 session (đúng 1 checkpoint/step, không ghi thừa).

**Vì sao chỉ idempotency key là chưa đủ?** Key khiến *provider* khử trùng lặp, nhưng nó
không cho worker biết việc gì đã xong. Sau một `WorkerCrash` giữa `issue_refund` và
`send_email`, worker mới vẫn phải chạy lại `classify_request` + `lookup_order` — đó là các
call LLM tốn tiền (`wasted_llm_calls`) — và nó không có cách nào biết còn phải làm bước nào.
Key bảo vệ *tiền*, checkpoint bảo vệ *công việc*.

**Vì sao chỉ checkpoint là chưa đủ?** Checkpoint chỉ được ghi *sau* khi step trả về. Cửa sổ
nguy hiểm là: `_call` đã thực sự chạy `op()` (tiền đã đi) rồi mới ném `ServiceTimeout`
hoặc `WorkerCrash` — worker mới đọc checkpoint thấy trống nên chạy lại payment. Không có
key ổn định thì lần chạy lại đó tạo **khoản hoàn tiền thứ hai thật**. Đo được ở naive:
128 duplicate refunds / 1 496 700 cents hoàn dư.

**Nếu ghi checkpoint *trước* khi gọi payment?** Đó là lỗi ngược lại và tệ hơn: worker mới
thấy "issue_refund đã xong" nên **bỏ qua** bước thanh toán trong khi tiền chưa hề đi →
`lost_refunds > 0`, khách không nhận được tiền và không ai biết. Thứ tự bắt buộc là
*side effect trước, checkpoint sau* (đúng thứ tự trong `run()`; có test
`test_checkpoint_is_never_written_for_a_step_that_did_not_finish` canh điều này).

## 5. Alerting & error budget

`python scripts/run_slo.py --alerter both`:

| Sự cố | Mong đợi | Naive alerter | Burn-rate alerter | Độ trễ phát hiện (burn-rate) |
|---|---|---|---:|---:|
| checkout_bug | page | ✓ (delay 1 min) | ✓ | 27 min |
| provider_outage | page | ✓ (delay 0 min) | ✓ | 4 min |
| deploy_blip | none | **✗ page nhầm** | ✓ không page | — |
| slow_leak | ticket | **✗ bỏ sót** | ✓ ticket | 1337 min |
| degraded_evening | page | ✓ (delay 214 min) | ✓ | 291 min |
| night_spike | none | **✗ page nhầm** | ✓ không page | — |
| small_bump | none | **✗ page nhầm** | ✓ không page | — |

naive: `pages=7 tickets=0 false_pages=3 → FAIL`. burn-rate: `pages=3 tickets=1 false_pages=0 → PASS`.

4 event sinh ra: `fast_burn_2pct_1h` page [3507, 3527) và [8764, 8774);
`budget_leak_10pct_3d` ticket [22037, 23794); `slow_burn_5pct_6h` page [30291, 30368).
Cửa sổ ngắn khiến page tự tắt vài phút sau khi hồi phục (3527 − 3525 = 2 phút cho
`checkout_bug`).

**Error budget 30 ngày:** `requests = 25 901 755`, `allowed_bad = 129 508.8`,
`bad = 55 618` → availability **0.997853**, đã tiêu **42.95 %**, còn **57.05 %**,
`freeze_releases = False` (chưa cần đóng băng release). Toàn bộ 55 618 lỗi nằm rải ở các
sự cố đã dán nhãn; riêng `slow_leak` chiếm 25 936 lỗi = **20.03 %** ngân sách tháng — đó
là lý do nó phải tạo ticket dù chỉ 1.5 % lỗi.

**Vì sao `small_bump` (20 phút ở 12 %) không nên đánh thức ai lúc 11 giờ trưa?** Đo trực
tiếp từ dữ liệu: nó gây **1 794 lỗi / 15 211 request = 1.385 % ngân sách tháng**. Luật
page nhanh nhất cần burn rate 14.4 × (tỷ lệ lỗi ≥ 7.2 % trong cửa sổ 1 giờ); 20 phút ở
12 % chỉ cho 20 × 0.12 / 60 = **4 %** ở cửa sổ 1 giờ ⇒ burn rate 8 × < 14.4 ⇒ không page.
Đổi lại, nếu cứ page thì on-call bị đánh thức vì ~1 % ngân sách, và sau vài lần như vậy
alert sẽ bị tắt tiếng — mất luôn khả năng phát hiện sự cố thật. Cùng logic:
`deploy_blip` 0.854 %, `night_spike` 0.406 % ngân sách → cũng không đáng page.

4 điểm còn thiếu trong `make grade` là `slo_hidden` (bộ CSV + nhãn SLO ẩn của giảng viên,
không có trong repo). Để chắc chắn phần này generalize, tôi tự sinh 40 bộ dữ liệu "ẩn giả"
(`scripts/robustness.py`: 10 lịch sự cố bị dịch ±137/−211/+401/+480 phút và scale cường độ
×1.0/×1.3, × 4 seed) — **40/40 PASS**, không false page, mọi sự cố page/ticket đúng hạn.

## 6. Postmortem — Băng G2 với client naive (blameless)

*Nguồn: `reports/gameday/naive_G2_overload_storm_timeseries.csv` và hình
`reports/gameday/G2_overload_storm.png` (`python scripts/plot_timeseries.py --scenario G2_overload_storm`).*

**Tóm tắt.** Trong 3 phút `alpha` mất 75 % GPU, `NaiveClient` đã nhân tải lên ~209
attempt/giây (so với ~6.7 attempt/giây lúc thường) và biến sự cố 3 phút của provider thành
**7 phút 35 giây sập toàn bộ**: 1 285 / 2 362 lượt user (45.6 %) thất bại. Hệ thống chỉ
phục hồi 155 giây **sau khi** provider đã hồi phục hoàn toàn. Nếu tốc độ lỗi 54.4 % đó
được duy trì, ngân sách lỗi 30 ngày sẽ cháy sạch trong ~6.6 giờ.

**Ảnh hưởng người dùng.** 1 285 lượt user thất bại (không hoàn tiền được và nhận câu trả
lời degraded), kéo dài từ t=120 đến t=455 giây ảo — 335 giây, tức **155 giây sau khi
provider hồi phục** vẫn không ai được phục vụ. Tỷ lệ lỗi trong băng là 1 − 0.456 = 54.4 %,
tức burn rate 54.4 % / 0.5 % = **108.8 ×**; nếu giả sử tháng đó chỉ có sự cố này thì
1 285 lượt lỗi ≈ 2.5 % ngân sách tháng (vì băng chỉ dài 10 phút), nhưng *tốc độ* tiêu thì
tương đương 108× và sẽ xoá sạch ngân sách trong 6.6 giờ. Chi phí: 4 983 đơn vị cho 1 077
lượt thành công = **4.627 cost/turn**, gấp 2.3× mức bình thường (2.008 ở G4) — trả tiền
cho 34 475 attempt trong khi chỉ 3 481 logical call.

**Timeline** (giây ảo, cửa sổ trượt 10 s, từ CSV):

| t | Sự kiện | Bằng chứng (số) |
|---:|---|---|
| 110 | Trạng thái bình thường | 73/73 lượt OK (100 %), 6.7 attempt/s, 100 % vào `alpha` |
| 119 | Ngay trước sự cố | 92/92 OK (100 %), 9.1 attempt/s |
| 121 | `alpha` mất 75 % năng lực | attempt/s vẫn 8.8; breaker naive không tồn tại |
| 150 | **Retry storm bùng phát** | 0/50 lượt OK (**0 %**), attempt/s = **208.9** (23× tải đến), 100 % vào `alpha` |
| 180 | Hệ thống đã tự khoá cứng | 0/32 OK (0 %), 114.8 attempt/s |
| 240 | Sự cố provider vẫn tiếp diễn | 0/56 OK (0 %), 138.2 attempt/s |
| 300 | **`alpha` hồi phục hoàn toàn** | 0/43 OK (**vẫn 0 %**), 142.9 attempt/s — hàng đợi toàn request client đã bỏ |
| 310 | Vẫn chưa hồi phục | 0/14 OK (0 %), 28.4 attempt/s |
| 360 | Vẫn chưa hồi phục | 0/36 OK (0 %), 21.2 attempt/s |
| 455 | Bắt đầu thoát | 12/40 OK (**30 %**), 13.5 attempt/s |
| 500 | Đã hồi phục | 93/93 OK (100 %), 9.6 attempt/s |
| 560 | Ổn định | 66/66 OK (100 %), 7.0 attempt/s |

Hình `G2_overload_storm.png` cho thấy đúng hai thứ: panel trên (attempts/s theo provider)
phình từ 7 lên ~209 rồi tắt dần rất chậm sau t=300; panel dưới (success ratio 10 s rolling)
rơi thẳng về 0 ở t≈140 và **nằm ở 0 suốt 115 giây sau khi đường attempts đã hạ** — khoảng
cách giữa hai panel đó chính là metastable failure.

**Trigger vs root cause.**
*Trigger:* một sự kiện hạ tầng — `alpha` mất 75 % GPU trong 3 phút (t=120–300). Đây chỉ là
*điều kiện khởi phát*, kết thúc ở t=300.
*Root cause:* kiến trúc retry của `NaiveClient` — **3 tầng retry chồng nhau**
(app ×4, gateway ×3, SDK ×3 = tới 36 attempt/call), delay cố định 0.2 s **không jitter**,
**không đọc `retry-after`** (đo được 28 381 vi phạm), **không breaker**, **không retry
budget**, **không biết deadline**. Khi provider chậm, mỗi call biến thành tối đa 36 request
mới; không jitter nên mọi client quay lại *cùng một thời điểm*; không budget nên không có
van nào đóng lại.

**Vì sao provider đã hồi phục mà hệ thống vẫn sập (metastable failure).** Vòng lặp tự duy
trì: (1) `alpha` chậm/529 ⇒ (2) client timeout sau 8 s nhưng **server vẫn tiếp tục xử lý**
request đã bị bỏ — công sức đó bị phí nhưng vẫn chiếm slot GPU; (3) client retry ⇒ thêm
request mới vào cùng hàng đợi FIFO; (4) hàng đợi dài hơn 10 s ⇒ server shed bằng 529 cho
cả những request *hợp lệ*; (5) nhiều 529 hơn ⇒ nhiều retry hơn. Khi GPU quay lại ở t=300,
hàng đợi đã chứa hàng nghìn request mà client đã bỏ; các request *mới* vẫn phải xếp sau
chúng, vượt 10 s và bị 529. Ở t=300 tải đến chỉ ~8 call/s nhưng đo được **142.9 attempt/s**
— tức hệ thống đang tự bơm tải vào chính nó. Vòng lặp chỉ tắt khi lượng retry giảm dần theo
cấp số nhân (mỗi lần hết 36 attempt lại bỏ một nhánh), đến t≈455 mới đủ hụt để hàng đợi
ngắn lại. Đây đúng là định nghĩa metastable failure của Bronson et al.: trigger đã biến mất
mà hệ thống vẫn ở trạng thái xấu, vì trạng thái xấu *tự nuôi chính nó*.

**Điều gì giúp / điều gì làm tệ hơn.**
*Làm tệ hơn:* 3 tầng retry nhân lên (×36); delay cố định không jitter (đồng loạt quay lại);
bỏ qua `retry-after`; không có breaker/fallback nên 100 % tải dồn vào đúng provider đang
hỏng; timeout không gắn với deadline user nên request được gửi đi cả khi không còn kịp.
*Giúp cải thiện (đã kiểm chứng, xem mục 1 và 3):* gộp về **một** tầng retry với full jitter
+ cap 4 s ⇒ G2 amplification 9.904 → 1.059; **retry budget** 0.1 token/request ⇒ retry bị
van lại (thí nghiệm 1: G2 amplification 1.675 → 1.378 khi bật); **circuit breaker** theo tỷ
lệ lỗi + slow-call ⇒ ngừng gửi vào provider đang hỏng (transition tại t=121.1, chỉ 1.1 s
sau trigger); **fallback ladder** có kiểm tra khả năng ⇒ tải sang `alpha-eu`/`beta` ⇒
availability 0.456 → **1.0**; **đọc đúng `retry-after`** ⇒ 28 381 vi phạm → **0**.

**Action items**

| # | Hành động | Loại | Owner | Cách kiểm chứng |
|---|---|---|---|---|
| 1 | Gộp 3 tầng retry (app/gateway/SDK) thành 1 tầng duy nhất ở client, có full jitter + cap 4 s + luôn đọc `retry-after`; tắt retry ở gateway và SDK | prevent | Platform / LLM Gateway | CI chạy G2: `amplification ≤ 1.2` và `retry_after_violations == 0` |
| 2 | Bật retry budget 0.1 token/request trong *mọi* đường gọi LLM (kể cả batch job), và coi việc thiếu budget là một metric | prevent | Platform / SRE | Test đơn vị `test_budget_caps_retries_to_a_ratio_of_requests`; cảnh báo khi retry vượt 10 % trong 5 phút |
| 3 | Alert burn-rate 14.4×/6×/1× (đã có ở Part E) trên chính SLI của gateway, không dùng ngưỡng 5 phút | detect | SRE / On-call | `python scripts/run_slo.py --alerter burnrate` phải ra `false_pages == 0` và page `provider_outage` trong ≤ 8 phút |
| 4 | Giới hạn số request *đang bay* tới mỗi provider (concurrency limit) và huỷ công việc của request client đã bỏ (deadline propagation xuống provider) | mitigate | Backend / LLM Gateway | G2 phải có `recovery_s ≤ 30`; đo số request server xử lý mà không ai nhận (wasted work) |
| 5 | Diễn tập Game Day định kỳ: replay G2 trên staging, yêu cầu recovery ≤ 30 s trước khi cho phép release | prevent | Tech Lead / Delivery | Lịch diễn tập hàng quý; gate trong pipeline release |

*(Blameless: sự cố này không do một cá nhân. Ba tầng retry là mặc định "an toàn" mà cả ba
team đều bật độc lập — app, gateway và SDK — và không ai thấy tổng số nhân lên. Lỗi thuộc
về hệ thống: thiếu một chỗ duy nhất sở hữu chính sách retry, thiếu thí nghiệm tải, và thiếu
metric/alert trên amplification.)*

## 7. Một điểm yếu còn lại

**G2 p95 = 11.911 s (naive là 15.034 s) — vẫn là con số xấu nhất trong 6 băng, dù
availability đã 1.0.** Nguyên nhân: breaker cần `min_calls: 10` kết quả trước khi trip, mà
10 kết quả đầu tiên của sự cố đều là *thành công chậm* (capacity còn 25 % ⇒ ~4.8 s/request
cộng thời gian xếp hàng). Những lượt user rơi đúng vào ~2 giây đầu đó phải chờ cả call
`tool` lẫn `chat` trên `alpha` ⇒ ~12 s. Breaker chỉ chuyển tải *sau khi* đã quan sát đủ 10
call chậm.

Cách sửa: **phát hiện suy giảm trước khi có đủ mẫu** — dùng cửa sổ nhỏ hơn cho slow-call
(ví dụ `min_calls: 5` cho nhánh slow, giữ `min_calls: 10` cho nhánh failure-rate), hoặc
thêm luật "3 call chậm liên tiếp" đóng vai trò fast-path, hoặc ngưỡng slow *thích ứng*
(p99 trượt của provider thay vì hằng số 5 s). Đánh đổi: ngưỡng thích ứng dễ trip nhầm ở
G1/G3 khi tải tăng làm p99 dịch lên — nên phải giữ nhánh failure-rate độc lập và đo lại
cả 6 băng sau mỗi thay đổi. Tôi chưa làm vì với 540 băng nhiễu cấu hình hiện tại đã PASS
100 % và mọi thay đổi ngưỡng đều có nguy cơ hồi quy ở băng khác.

Điểm yếu thứ hai, nhỏ hơn: G5 availability chỉ 0.8179 (trần yêu cầu 0.78) vì 60 % request
malformed trong 2 phút là lỗi của chính deploy — client đúng khi GiveUp ngay
(`client_error_retries = 0`, `unneeded_failovers = 0`), nhưng con số này sẽ không bao giờ
cao. Đây là giới hạn của bài toán, không phải của thiết kế.

## 8. Cache (Part G)

`python scripts/run_gameday.py --scenarios configs/scenarios_cache.yaml --client resilient,cached --out reports/cache`
(seed 25; đã kiểm 12 seed và 30 băng C1 nhiễu — xem dưới):

| Client | availability | faq_availability | cache_hit_ratio | stale_served | wrong_answers | outdated_answers | cost/turn | PASS? |
|---|---:|---:|---:|---:|---:|---:|---:|:---:|
| resilient (không cache) | 0.8373 | 0.8424 | 0.0 | 0 | 0 | 0 | 1.386 | ✗ |
| cached (của tôi) | **0.9124** | **0.9645** | **0.592** | 192 | **0** | **2** | **0.955** | ✓ |

Ở 12 seed: `wrong_answers = 0` tuyệt đối, `outdated_answers` ∈ [1, 10] (trần 15),
`cache_hit_ratio` ∈ [0.567, 0.594] (sàn 0.50), `faq_availability` ∈ [0.950, 0.965] (sàn 0.93).
`cost/turn` giảm 1.386 → 0.955 (−31 %) — cache không chỉ cứu availability mà còn rẻ hơn.

| Tham số `cache:` | Giá trị | Vì sao (số liệu nào) |
|---|---:|---|
| `similarity_threshold` | 0.91 | Đo trực tiếp trên catalog embedding (`gameday_lab.sim.faq`): cosine giữa hai cách diễn đạt **cùng** intent thấp nhất = **0.9507**; cosine cao nhất giữa hai intent **khác** nhau = **0.8731** (`shipping_time` ↔ `my_order_status`). 0.91 là trung điểm khoảng trống, chừa ~0.04 về cả hai phía. Ở 0.80 (starter) `wrong_answers = 182`, toàn bộ là `other_question`; ở 0.97 hit ratio rơi còn 0.495 < 0.50. |
| `ttl_s` | 30 | `outdated_answers` chỉ bị chặn bởi TTL (thay đổi không webhook). Số lỗi kỳ vọng ≈ `shipping_serve_rate × ttl/2` ≈ 0.435 × 15 ≈ 6.5 trong trần 15. Quét `silent_updates` từ 430→560 s (dịch khỏi lưới TTL): ttl=30 → worst 12, ttl=45 → worst 14 (sát trần), **ttl=60 → worst 23 (FAIL)**. Đổi lại hit ratio 0.592 ≥ 0.50 với biên 18 %. |
| `max_stale_s` | 600 | Sự cố ở C1 kéo dài 120 s; entry có thể đã cũ tới `ttl + 120 = 150 s` khi provider quay lại. 600 phủ được cả một sự cố ~9 phút (kịch bản ẩn có thể dài hơn). Với `max_stale_s: 0` (starter) `faq_availability` chỉ 0.8587 < 0.93 — đây chính là chỗ mất 6 điểm băng C1. |
| `max_entries` | 500 | Chỉ ~18 intent dùng chung × 2 scope prompt = ~36 entry sống; 500 là biên rộng, LRU chỉ là lưới an toàn. |

**Thí nghiệm ngưỡng** (seed 25, giữ `ttl_s=30, max_stale_s=600`, chỉ đổi ngưỡng):

| `similarity_threshold` | cache_hit_ratio | wrong_answers | outdated | cost/turn | Kết quả |
|---|---:|---:|---:|---:|---|
| 0.80 (starter) | 0.587 | **182** (`other_question`) | 2 | 1.033 | FAIL `wrong_answers` |
| **0.91 (chọn)** | **0.592** | **0** | 2 | 0.955 | **PASS** |
| 0.97 | **0.495** | 0 | 2 | 1.020 | FAIL `cache_hit_ratio` |

Ở 0.80, các cặp "gần mà khác" bị trộn: `shipping_fee_city` ↔ `shipping_fee_province`
(0.8409) và `payment_methods` ↔ `cod` (0.8465) vượt ngưỡng, nên khách hỏi "phí ship nội
thành" nhận câu trả lời "phí ship đi tỉnh" — 182 lượt `other_question`. Ở 0.97 thì
`wrong_answers` vẫn 0 nhưng **hit ratio rơi xuống 0.495 < 0.50** — vì biên trên chỉ còn
0.9507 − 0.97 < 0, các cặp paraphrase hợp lệ bắt đầu trượt và mỗi lần trượt là một lần
gọi provider. Đây là lý do ngưỡng tốt nhất là trung điểm khoảng trống (0.91), không phải
"sát trên càng tốt".

**Thí nghiệm TTL** (0.91 / 600): `ttl_s` 30 → outdated **2**, hit 0.592;
60 → outdated 3, hit 0.652; 300 → outdated **95** (FAIL), hit 0.731.
**Webhook không giúp được gì cho `outdated_answers`**: `on_content_update` chỉ bắn khi CMS
*báo* tài liệu đổi (sự kiện t=150 `policy/refund`); thay đổi âm thầm ở t=480
(`policy/shipping`, bảng phí ship) không có webhook nên client không có tín hiệu nào để so
sánh — `Answer` không mang số phiên bản tài liệu. Vì vậy TTL là **thứ duy nhất** giới hạn
nó, và TTL phải ngắn. (Ngược lại, TTL dài đúng là nguyên nhân 95 `outdated_answers` ở
ttl=300 — gấp ~6× trần 15.)

**Thí nghiệm stale-if-error** (`max_stale_s: 0`): `faq_availability` 0.9645 → **0.8529**
(−11.2 điểm phần trăm, FAIL sàn 0.93), `cache_hit_ratio` 0.592 → **0.481** (cũng FAIL),
`stale_served` 192 → 0, availability tổng 0.9124 → **0.844**. Trong 2 phút mọi provider
sập, câu hỏi FAQ dùng chung không còn nguồn nào trả lời nếu không có nhánh stale — đúng
"bậc 3 cached response" của fallback ladder.

**Vì sao câu hỏi cá nhân ("Đơn #48213 của tôi đang ở đâu?") không được cache, kể cả khi
cache theo từng user?** Vì `personal=True` bị chặn ngay ở `lookup` *và* `store`, không chỉ
theo `user_id`. Hai lý do: (a) câu trả lời phụ thuộc **trạng thái sống** (đơn đang ở kho
nào, tiền hoàn đã về chưa, còn bao nhiêu điểm) — TTL 30 s vẫn đủ để trả lời sai; (b) các
câu hỏi cá nhân chỉ khác nhau ở **số đơn**, nên cosine giữa câu của hai user là ~0.99 —
nếu chỉ scope theo user, một lỗi trong việc suy ra user (hoặc một paraphrase bị gán sai)
sẽ lộ đơn hàng của người khác. Chặn theo *loại câu hỏi* là biện pháp an toàn theo cấu trúc,
không phụ thuộc vào việc gán danh tính đúng. Đo được: `wrong_answers_by_reason` không hề
xuất hiện `other_user` hay `personal_cached`.

**Trong lúc mọi provider sập, dashboard vẫn xanh nhờ cache. Alert dựa trên số nào?**
Không alert trên `availability` tổng (nó vẫn 0.9124 — và trong một sự cố dài hơn, tỷ lệ này
có thể nằm trong SLO dù 100 % provider đang chết). Phải alert trên **nguồn gốc, không phải
triệu chứng**: (1) burn-rate SLO (mục 5) — nhưng đo trên SLI *provider*, không đo trên tỷ lệ
lượt user hài lòng; và (2) các bộ đếm chất lượng cache mà tôi đã in ra: `stale_served > 0`
là tín hiệu "đang phục vụ dữ liệu cũ vì không provider nào trả lời" (ở C1: 192 lượt), cùng
với `cache_hit_ratio` tăng vọt và số provider khả dụng = 0. Alert "stale_served > 0 trong
5 phút" là alert cần thiết ở đây; còn `wrong_answers > 0` phải là alert **page mức cao
nhất** vì đó là trả lời sai cho khách, không phải giảm chất lượng.

---

## Phụ lục — Tái lập

```bash
python -m venv .venv && .venv/Scripts/activate        # Windows
pip install -e ".[dev,plot]"
python -m pytest -q                                    # 148 passed
python scripts/run_gameday.py --client both            # naive 0/6, resilient 6/6
python scripts/run_workflows.py --executor both
python scripts/run_slo.py --alerter both
python scripts/run_gameday.py --scenarios configs/scenarios_cache.yaml --client resilient,cached --out reports/cache
python scripts/plot_timeseries.py --scenario G2_overload_storm
python scripts/experiments.py --seeds 25,26,27         # 3 thí nghiệm + quét cache -> reports/experiments.json
python scripts/robustness.py                           # 540 băng nhiễu + 30 băng C1 + 40 bộ SLO ẩn giả
python scripts/grade.py --seeds 25,26,27               # 84/88 (4 điểm là SLO ẩn)
ruff check src tests scripts
```

Bộ test gồm 82 test gốc + **66 test biên tôi thêm** trong `tests/test_robustness_extra.py`
(thời gian ảo không tăng, permit cũ về muộn, probe half-open hoàn thành không theo thứ tự,
similarity đúng bằng ngưỡng, invalidation chen giữa GiveUp và stale serve, prompt version
đổi, crash ngay sau refund/email, error budget đúng biên freeze, zero/empty traffic).
