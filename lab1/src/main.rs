use std::cmp::Ordering;

#[derive(Clone, Debug)]
struct Job {
    id: usize,
    arrival: f64, // 到达时间，分钟
    service: f64, // 估计运行时间，分钟
    start: Option<f64>,
    end: Option<f64>,
}

impl Job {
    fn new(id: usize, arrival: f64, service: f64) -> Self {
        Self { id, arrival, service, start: None, end: None }
    }

    fn turnaround(&self) -> Option<f64> {
        match (self.end, Some(self.arrival)) {
            (Some(e), Some(a)) => Some(e - a),
            _ => None,
        }
    }

    fn weighted_turnaround(&self) -> Option<f64> {
        match (self.turnaround(), self.service) {
            (Some(t), s) if s > 0.0 => Some(t / s),
            _ => None,
        }
    }
}

// 结果打印辅助
fn print_results(mut jobs: Vec<Job>, title: &str) {
    jobs.sort_by(|a, b| a.id.cmp(&b.id));
    println!("\n=== {} ===", title);
    println!("id\tarr\tserv\tstart\tend\tturn\twturn");
    let mut sum_turn = 0.0;
    let mut sum_wturn = 0.0;
    let mut count = 0.0;
    for j in &jobs {
        let start = j.start.map_or(-1.0, |v| v);
        let end = j.end.map_or(-1.0, |v| v);
        let turn = j.turnaround().unwrap_or(-1.0);
        let wturn = j.weighted_turnaround().unwrap_or(-1.0);
        println!("{}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}", j.id, j.arrival, j.service, start, end, turn, wturn);
        if turn >= 0.0 {
            sum_turn += turn;
            sum_wturn += wturn.max(0.0);
            count += 1.0;
        }
    }
    if count > 0.0 {
        println!("平均周转时间 = {:.4}", sum_turn / count);
        println!("带权平均周转时间 = {:.4}", sum_wturn / count);
    }
}

// 分配到多道：返回各作业的 start/end
// 采用非抢占式（批处理作业）调度。m 为道数（CPU 数）

// 1) FCFS：按到达时间排序，作业到达后尽快分配给空闲道或等待道空
fn schedule_fcfs(jobs: &[Job], m: usize) -> Vec<Job> {
    let mut jobs: Vec<Job> = jobs.to_vec();
    jobs.sort_by(|a, b| a.arrival.partial_cmp(&b.arrival).unwrap_or(Ordering::Equal));

    // 每条道的可用时间
    let mut core_free: Vec<f64> = vec![0.0; m];

    for j in &mut jobs {
        // 找到最早空闲的道及时间
        let (idx, &free_t) = core_free.iter().enumerate().min_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(Ordering::Equal)).unwrap();
        // 作业实际开始时间是 max(到达, 道空闲)
        let start = j.arrival.max(free_t);
        let end = start + j.service;
        j.start = Some(start);
        j.end = Some(end);
        core_free[idx] = end; // 更新道空闲时间
    }
    jobs
}

// 2) SJF（非抢占）：在每次调度时，从已到达且未完成的作业中选择service最小者分配到空闲道
fn schedule_sjf(jobs: &[Job], m: usize) -> Vec<Job> {
    let mut all: Vec<Job> = jobs.to_vec();
    let n = all.len();
    // 按到达时间排序用于发现新到达
    all.sort_by(|a, b| a.arrival.partial_cmp(&b.arrival).unwrap_or(Ordering::Equal));

    let mut time = 0.0f64;
    let mut finished: Vec<Job> = Vec::with_capacity(n);
    let mut ready: Vec<Job> = Vec::new();
    let mut idx_next = 0; // 下一个未放入ready的作业索引

    // 多道：track core_free times
    let mut core_free: Vec<f64> = vec![0.0; m];

    // 事件驱动：当有空闲道且ready不空时分配；否则推进时间到下一个到达或下一个道空闲
    while finished.len() < n {
        // 将已到达的作业加入 ready
        while idx_next < all.len() && all[idx_next].arrival <= time {
            ready.push(all[idx_next].clone());
            idx_next += 1;
        }

        // 找空闲道
        let mut free_idxs: Vec<usize> = core_free.iter().enumerate().filter(|(_, &t)| t <= time + 1e-9).map(|(i, _)| i).collect();

        if free_idxs.is_empty() {
            // 没有空闲道
            if !ready.is_empty() {
                // all busy but ready有作业，推进到最近道空闲时间
                if let Some(&next_free) = core_free.iter().min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal)) {
                    time = next_free;
                    continue;
                }
            } else {
                // ready 为空：推进到下一个到达或下一个空闲
                let next_arrival = all.get(idx_next).map(|j| j.arrival);
                let next_free = core_free.iter().cloned().min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
                match (next_arrival, next_free) {
                    (Some(na), Some(nf)) => time = na.min(nf),
                    (Some(na), None) => time = na,
                    (None, Some(nf)) => time = nf,
                    (None, None) => break,
                }
                continue;
            }
        }

        // 有空闲道且ready可能为空
        if ready.is_empty() {
            // 若 ready 为空，推进到下一个到达
            if let Some(j) = all.get(idx_next) {
                time = j.arrival;
                continue;
            } else {
                break;
            }
        }

        // 对 ready 按 service 升序选择最短作业分配
        ready.sort_by(|a, b| a.service.partial_cmp(&b.service).unwrap_or(Ordering::Equal));

        for core in free_idxs {
            if ready.is_empty() { break; }
            let mut job = ready.remove(0);
            let start = time.max(job.arrival);
            let end = start + job.service;
            job.start = Some(start);
            job.end = Some(end);
            finished.push(job.clone());
            core_free[core] = end;
        }

        // time 可能保持不变，这里推进到下一个事件以避免死循环
        // 下一个事件是最早的: 下一个到达或最早道空闲
        let next_arrival = all.get(idx_next).map(|j| j.arrival);
        let next_free = core_free.iter().cloned().min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        match (next_arrival, next_free) {
            (Some(na), Some(nf)) => time = na.min(nf),
            (Some(na), None) => time = na,
            (None, Some(nf)) => time = nf,
            (None, None) => break,
        }
    }

    // finished 按 id 返回：某些作业可能尚在 ready（极少），把它们也添加并设置时间
    // 为安全，若仍有ready未分配，逐一分配到最早空闲道
    if !ready.is_empty() {
        for job in ready.into_iter() {
            let (idx, &free_t) = core_free.iter().enumerate().min_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(Ordering::Equal)).unwrap();
            let start = job.arrival.max(free_t);
            let end = start + job.service;
            let mut j = job.clone();
            j.start = Some(start);
            j.end = Some(end);
            core_free[idx] = end;
            finished.push(j);
        }
    }

    // 结果按 id 排序返回
    finished.sort_by(|a, b| a.id.cmp(&b.id));
    finished
}

// 3) HRRN（响应比最高者优先）：在每次分配时选响应比最大的作业
fn schedule_hrrn(jobs: &[Job], m: usize) -> Vec<Job> {
    let mut all: Vec<Job> = jobs.to_vec();
    let n = all.len();
    all.sort_by(|a, b| a.arrival.partial_cmp(&b.arrival).unwrap_or(Ordering::Equal));

    let mut time = 0.0f64;
    let mut finished: Vec<Job> = Vec::with_capacity(n);
    let mut ready: Vec<Job> = Vec::new();
    let mut idx_next = 0;
    let mut core_free: Vec<f64> = vec![0.0; m];

    while finished.len() < n {
        while idx_next < all.len() && all[idx_next].arrival <= time {
            ready.push(all[idx_next].clone());
            idx_next += 1;
        }

        let mut free_idxs: Vec<usize> = core_free.iter().enumerate().filter(|(_, &t)| t <= time + 1e-9).map(|(i, _)| i).collect();

        if free_idxs.is_empty() {
            if !ready.is_empty() {
                if let Some(&next_free) = core_free.iter().min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal)) {
                    time = next_free;
                    continue;
                }
            } else {
                let next_arrival = all.get(idx_next).map(|j| j.arrival);
                let next_free = core_free.iter().cloned().min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
                match (next_arrival, next_free) {
                    (Some(na), Some(nf)) => time = na.min(nf),
                    (Some(na), None) => time = na,
                    (None, Some(nf)) => time = nf,
                    (None, None) => break,
                }
                continue;
            }
        }

        if ready.is_empty() {
            if let Some(j) = all.get(idx_next) {
                time = j.arrival;
                continue;
            } else { break; }
        }

        // 计算响应比： (waiting + service) / service = (time - arrival + service)/service
        ready.sort_by(|a, b| {
            let ra = (time - a.arrival + a.service) / a.service;
            let rb = (time - b.arrival + b.service) / b.service;
            rb.partial_cmp(&ra).unwrap_or(Ordering::Equal)
        });

        for core in free_idxs {
            if ready.is_empty() { break; }
            let mut job = ready.remove(0);
            let start = time.max(job.arrival);
            let end = start + job.service;
            job.start = Some(start);
            job.end = Some(end);
            finished.push(job.clone());
            core_free[core] = end;
        }

        let next_arrival = all.get(idx_next).map(|j| j.arrival);
        let next_free = core_free.iter().cloned().min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        match (next_arrival, next_free) {
            (Some(na), Some(nf)) => time = na.min(nf),
            (Some(na), None) => time = na,
            (None, Some(nf)) => time = nf,
            (None, None) => break,
        }
    }

    if !ready.is_empty() {
        for job in ready.into_iter() {
            let (idx, &free_t) = core_free.iter().enumerate().min_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(Ordering::Equal)).unwrap();
            let start = job.arrival.max(free_t);
            let end = start + job.service;
            let mut j = job.clone();
            j.start = Some(start);
            j.end = Some(end);
            core_free[idx] = end;
            finished.push(j);
        }
    }

    finished.sort_by(|a, b| a.id.cmp(&b.id));
    finished
}

// 用于生成样例作业流
fn sample_jobs() -> Vec<Job> {
    vec![
        Job::new(1, 0.0, 3.0),
        Job::new(2, 2.0, 6.0),
        Job::new(3, 4.0, 4.0),
        Job::new(4, 6.0, 5.0),
        Job::new(5, 8.0, 2.0),
    ]
}

// 另一组用于衡量算法性能的流（包含多个短作业与长作业）
fn sample_jobs2() -> Vec<Job> {
    vec![
        Job::new(1, 0.0, 8.0),
        Job::new(2, 1.0, 4.0),
        Job::new(3, 2.0, 9.0),
        Job::new(4, 3.0, 5.0),
        Job::new(5, 10.0, 2.0),
        Job::new(6, 10.0, 1.0),
    ]
}

fn main() {
    // 单道（m = 1）
    let jobs = sample_jobs();
    let res_fcfs = schedule_fcfs(&jobs, 1);
    print_results(res_fcfs, "FCFS - 单道");

    let res_sjf = schedule_sjf(&jobs, 1);
    print_results(res_sjf, "SJF - 单道");

    let res_hrrn = schedule_hrrn(&jobs, 1);
    print_results(res_hrrn, "HRRN - 单道");

    // 多道（m = 2）
    let jobs2 = sample_jobs();
    let res_fcfs_2 = schedule_fcfs(&jobs2, 2);
    print_results(res_fcfs_2, "FCFS - 双道");

    let res_sjf_2 = schedule_sjf(&jobs2, 2);
    print_results(res_sjf_2, "SJF - 双道");

    let res_hrrn_2 = schedule_hrrn(&jobs2, 2);
    print_results(res_hrrn_2, "HRRN - 双道");

    // 对不同作业流衡量同一算法
    let stream_a = sample_jobs();
    let stream_b = sample_jobs2();
    println!("\n=== 同一算法在不同作业流上的比较（示例） ===");
    let a_fcfs = schedule_fcfs(&stream_a, 1);
    let b_fcfs = schedule_fcfs(&stream_b, 1);
    print_results(a_fcfs, "Stream A - FCFS - 单道");
    print_results(b_fcfs, "Stream B - FCFS - 单道");

}
