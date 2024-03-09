import TopNav from '@/app/ui/dashboard/topnav';

export default function Layout({ children }: { children: React.ReactNode }) {
    return (
      <div className='mt-20'>
            <TopNav />
            <div className='dashboard-size dashboard-bg mx-auto'>{children}</div>
      </div>
    );
  }